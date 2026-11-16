use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    time::Duration,
};

use futures::future;
use tokio::{
    sync::{mpsc, mpsc::error::TryRecvError, oneshot::Sender},
    time::Instant,
};

use crate::{
    adaptors::throttle::{request_lock::RequestLock, ChatIdHash, Limits, Settings},
    errors::AsResponseParameters,
    requests::Requester,
};

const MINUTE: Duration = Duration::from_secs(60);
const SECOND: Duration = Duration::from_secs(1);

// Delay between worker iterations.
//
// For now it's `second/4`, but that number is chosen pretty randomly, we may
// want to change this.
const DELAY: Duration = Duration::from_millis(250);

/// Minimal time between calls to queue_full function
const QUEUE_FULL_DELAY: Duration = Duration::from_secs(4);

#[derive(Debug)]
pub(super) enum InfoMessage {
    GetLimits { response: Sender<Limits> },
    SetLimits { new: Limits, response: Sender<()> },
}

/// A request waiting for the worker's permission to be sent.
pub(super) struct QueuedRequest {
    pub(super) chat: ChatIdHash,
    pub(super) lock: RequestLock,
    pub(super) queued_at: Instant,
}

type RequestsSent = u32;

// I wish there was special data structure for history which removed the
// need in 2 hashmaps
// (waffle)
#[derive(Default)]
struct RequestsSentToChats {
    per_min: HashMap<ChatIdHash, RequestsSent>,
    per_sec: HashMap<ChatIdHash, RequestsSent>,
}

pub(super) struct FreezeUntil {
    pub(super) until: Instant,
    pub(super) after: Duration,
    pub(super) chat: ChatIdHash,
}

/// Pending requests, queued per chat.
///
/// A chat over its limits holds back only its own requests: the others are
/// found by walking `order`, not by scanning past everything it has queued.
#[derive(Default)]
struct Backlog {
    chats: HashMap<ChatIdHash, VecDeque<QueuedRequest>>,
    /// Chats with pending requests, in the order they got the first one.
    order: Vec<ChatIdHash>,
    len: usize,
}

/// What the worker woke up for while waiting for requests.
enum Wake {
    Freeze(Option<FreezeUntil>),
    Info(Option<InfoMessage>),
    Read,
}

/// Chats refused with `RetryAfter`, and the whole bot when enough of them are.
struct Freezes {
    chats: HashMap<ChatIdHash, Instant>,
    global_until: Option<Instant>,
    global_threshold: u32,
}

// Throttling is quite complicated. This comment describes the algorithm of the
// current implementation.
//
// ### Request
//
// When a throttling request is sent, it sends its `ChatId`, a `Sender<()>` and
// the time it was queued at to the worker. Then the request waits for a
// notification from the worker. When notification is received, it sends the
// underlying request.
//
// ### Worker
//
// The worker does the most important job -- it ensures that the limits are
// never exceeded.
//
// The worker stores a history of requests sent in the last minute (and to which
// chats they were sent) and a backlog of pending requests, queued per chat.
//
// The worker does the following algorithm loop:
//
// 1. If the backlog is empty, wait for the first message in incoming channel
// (and add it to the backlog).
//
// 2. Read the present messages from an incoming channel and transfer them to
// the backlog of their chats.
//
// 3. Record the current time, forget the freezes that have run out.
//
// 4. If the whole bot is frozen, wait a bit and `continue` to the next
// iteration.
//
// 5. Clear the history from records whose time < (current time - minute).
//
// 6. Count all requests which were sent last second, `allowed =
// limit.messages_per_sec_overall - count`.
//
// 7. If `allowed == 0` wait a bit and `continue` to the next iteration.
//
// 8. Count how many requests were sent to which chats (i.e.: create
// `Map<ChatId, Count>`). (Note: the same map, but for last minute also exists,
// but it's updated, instead of recreation.)
//
// 9. Walk the chats with pending requests, oldest first. For a chat that isn't
// frozen and hasn't exceeded the limits (i.e.: map[chat] < limit), notify its
// oldest request that it can be now executed, decrease `allowed`, increase
// counts, add record to the history. Stop when `allowed == 0`.
pub(super) async fn worker<B>(
    Settings {
        mut limits,
        mut on_queue_full,
        mut on_wait,
        retry,
        check_slow_mode,
        global_freeze_chats,
    }: Settings,
    mut rx: mpsc::Receiver<QueuedRequest>,
    mut info_rx: mpsc::Receiver<InfoMessage>,
    bot: B,
) where
    B: Requester,
    B::Err: AsResponseParameters,
{
    let mut backlog = Backlog::default();

    let mut history: VecDeque<(ChatIdHash, Instant)> = VecDeque::new();
    let mut requests_sent = RequestsSentToChats::default();

    let mut slow_mode: Option<HashMap<ChatIdHash, (Duration, Instant)>> =
        check_slow_mode.then(HashMap::new);

    let mut freezes = Freezes::new(global_freeze_chats);

    let mut rx_is_closed = false;

    let mut last_queue_full =
        Instant::now().checked_sub(QUEUE_FULL_DELAY).unwrap_or_else(Instant::now);

    let (freeze_tx, mut freeze_rx) = mpsc::channel::<FreezeUntil>(1);

    let mut info_is_closed = false;

    while !rx_is_closed || !backlog.is_empty() {
        answer_info(&mut info_rx, &mut limits);

        // Reading more than the overall limit allows to send in a tick keeps the
        // channel moving without letting the backlog grow faster than needed.
        let intake = (limits.messages_per_sec_overall as usize).max(1);

        // While the backlog is empty, `read_from_rx` waits for a request, so the
        // freezes and the `InfoMessage`s are waited for alongside it.
        loop {
            let wake = tokio::select! {
                biased;
                until = freeze_rx.recv() => Wake::Freeze(until),
                info = recv_info(&mut info_rx, info_is_closed) => Wake::Info(info),
                () = read_from_rx(&mut rx, &mut backlog, intake, &mut rx_is_closed) => Wake::Read,
            };

            match wake {
                Wake::Freeze(until) => {
                    freeze(&mut freeze_rx, slow_mode.as_mut(), &bot, &mut freezes, until).await;
                }
                Wake::Info(Some(info)) => answer(info, &mut limits),
                Wake::Info(None) => info_is_closed = true,
                Wake::Read => break,
            }
        }

        // _Maybe_ we need to use `spawn_blocking` here, because there is
        // decent amount of blocking work. However _for now_ I've decided not
        // to use it here.
        //
        // Reasons (not to use `spawn_blocking`):
        //
        // 1. The work seems not very CPU-bound, it's not heavy computations, it's more
        //    like light computations.
        //
        // 2. `spawn_blocking` is not zero-cost — it spawns a new system thread
        //    + do so other work. This may actually be *worse* then current
        //    "just do everything in this async fn" approach.
        //
        // 3. With `rt-threaded` feature, tokio uses [`num_cpus()`] threads which should
        //    be enough to work fine with one a-bit-blocking task. Crucially current
        //    behaviour will be problem mostly with single-threaded runtimes (and in
        //    case you're using one, you probably don't want to spawn unnecessary
        //    threads anyway).
        //
        // I think if we'll ever change this behaviour, we need to make it
        // _configurable_.
        //
        // See also [discussion (ru)].
        //
        // NOTE: If you are reading this because you have any problems because
        // of this worker, open an [issue on github]
        //
        // [`num_cpus()`]: https://vee.gg/JGwq2
        // [discussion (ru)]: https://t.me/rust_async/27891
        // [issue on github]: https://github.com/teloxide/teloxide/issues/new
        //
        // (waffle)

        let now = Instant::now();
        freezes.expire(now);

        if freezes.is_global(now) {
            tokio::time::sleep(DELAY).await;
            continue;
        }

        let min_back = now.checked_sub(MINUTE).unwrap_or(now);
        let sec_back = now.checked_sub(SECOND).unwrap_or(now);

        // make history and requests_sent up-to-date
        while let Some((_, time)) = history.front() {
            // history is sorted, we found first up-to-date thing
            if time >= &min_back {
                break;
            }

            if let Some((chat, _)) = history.pop_front() {
                let entry = requests_sent.per_min.entry(chat).and_modify(|count| {
                    *count -= 1;
                });

                if let Entry::Occupied(entry) = entry {
                    if *entry.get() == 0 {
                        entry.remove_entry();
                    }
                }
            }
        }

        // as truncates which is ok since in case of truncation it would always be >=
        // limits.overall_s
        let used = history.iter().rev().take_while(|(_, time)| time > &sec_back).count() as u32;
        let mut allowed = limits.messages_per_sec_overall.saturating_sub(used);

        if allowed == 0 {
            requests_sent.per_sec.clear();
            tokio::time::sleep(DELAY).await;
            continue;
        }

        for (chat, _) in history.iter().rev().take_while(|(_, time)| time > &sec_back) {
            *requests_sent.per_sec.entry(*chat).or_insert(0) += 1;
        }

        let mut position = 0;
        let mut queue_full = false;

        while let Some(&chat) = backlog.order.get(position) {
            if !may_send(&chat, now, &limits, &requests_sent, slow_mode.as_ref(), &freezes) {
                position += 1;
                continue;
            }

            if allowed == 0 {
                queue_full = true;
                break;
            }

            // The same chat is looked at again: its limits may allow one more.
            let Some(request) = backlog.pop_front(position) else { continue };

            // Only count request as sent if the request wasn't dropped before unlocked
            if request.lock.unlock(retry, freeze_tx.clone()).is_ok() {
                on_wait(chat.kind(), request.queued_at.elapsed());

                *requests_sent.per_sec.entry(chat).or_insert(0) += 1;
                *requests_sent.per_min.entry(chat).or_insert(0) += 1;
                history.push_back((chat, Instant::now()));

                if let Some((_, last)) = slow_mode.as_mut().and_then(|sm| sm.get_mut(&chat)) {
                    *last = Instant::now();
                }

                // We have "sent" one request, so now we can send one less.
                allowed -= 1;
            }
        }

        if queue_full && last_queue_full.elapsed() > QUEUE_FULL_DELAY {
            last_queue_full = Instant::now();
            tokio::spawn(on_queue_full(backlog.len()));
        }

        // It's easier to just recompute last second stats, instead of keeping
        // track of it alongside with minute stats, so we just throw this away.
        requests_sent.per_sec.clear();
        tokio::time::sleep(DELAY).await;
    }
}

impl Backlog {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, request: QueuedRequest) {
        let chat = request.chat;
        let queue = self.chats.entry(chat).or_insert_with(|| {
            self.order.push(chat);
            VecDeque::new()
        });
        queue.push_back(request);
        self.len += 1;
    }

    /// Takes the oldest request of the chat at `position` in `order`, and
    /// forgets the chat once it has nothing left.
    fn pop_front(&mut self, position: usize) -> Option<QueuedRequest> {
        let chat = self.order[position];
        let Entry::Occupied(mut entry) = self.chats.entry(chat) else {
            self.order.remove(position);
            return None;
        };

        let request = entry.get_mut().pop_front();
        if entry.get().is_empty() {
            entry.remove();
            self.order.remove(position);
        }
        if request.is_some() {
            self.len -= 1;
        }
        request
    }
}

impl Freezes {
    fn new(global_threshold: u32) -> Self {
        Self { chats: HashMap::new(), global_until: None, global_threshold }
    }

    fn freeze(&mut self, chat: ChatIdHash, until: Instant, after: Duration) {
        self.expire(Instant::now());

        let chat_until = self.chats.entry(chat).or_insert(until);
        *chat_until = (*chat_until).max(until);

        let threshold = self.global_threshold as usize;
        if threshold == 0 || self.chats.len() < threshold {
            log::debug!(
                "freezing chat {chat:?} for approximately {after:?} due to `RetryAfter` error \
                 from telegram"
            );
            return;
        }

        let latest = self.chats.values().copied().max().unwrap_or(until);
        if self.global_until.is_none() {
            log::warn!(
                "freezing the bot for approximately {after:?} due to `RetryAfter` errors from \
                 telegram in {} chats",
                self.chats.len()
            );
        }
        self.global_until = Some(self.global_until.map_or(latest, |current| current.max(latest)));
    }

    fn expire(&mut self, now: Instant) {
        self.chats.retain(|_, until| *until > now);

        if self.global_until.is_some_and(|until| until <= now) {
            self.global_until = None;
            log::warn!("unfreezing the bot");
        }
    }

    fn is_global(&self, now: Instant) -> bool {
        self.global_until.is_some_and(|until| until > now)
    }

    fn is_frozen(&self, chat: &ChatIdHash, now: Instant) -> bool {
        self.is_global(now) || self.chats.get(chat).is_some_and(|until| *until > now)
    }
}

fn answer_info(rx: &mut mpsc::Receiver<InfoMessage>, limits: &mut Limits) {
    while let Ok(req) = rx.try_recv() {
        answer(req, limits);
    }
}

fn answer(req: InfoMessage, limits: &mut Limits) {
    // Errors are ignored with .ok(). Error means that the response channel
    // is closed and the response isn't needed.
    match req {
        InfoMessage::GetLimits { response } => response.send(*limits).ok(),
        InfoMessage::SetLimits { new, response } => {
            *limits = new;
            response.send(()).ok()
        }
    };
}

/// Waits for the next `InfoMessage`, or for ever once every sender is gone.
async fn recv_info(rx: &mut mpsc::Receiver<InfoMessage>, closed: bool) -> Option<InfoMessage> {
    if closed {
        future::pending().await
    } else {
        rx.recv().await
    }
}

fn may_send(
    chat: &ChatIdHash,
    now: Instant,
    limits: &Limits,
    requests_sent: &RequestsSentToChats,
    slow_mode: Option<&HashMap<ChatIdHash, (Duration, Instant)>>,
    freezes: &Freezes,
) -> bool {
    if freezes.is_frozen(chat, now) {
        return false;
    }

    if let Some(&(delay, last)) = slow_mode.and_then(|sm| sm.get(chat)) {
        if last + delay > Instant::now() {
            return false;
        }
    }

    let requests_sent_per_sec_count = requests_sent.per_sec.get(chat).copied().unwrap_or(0);
    let requests_sent_per_min_count = requests_sent.per_min.get(chat).copied().unwrap_or(0);

    let messages_per_min_limit = if chat.is_channel_or_supergroup() {
        limits.messages_per_min_channel_or_supergroup
    } else {
        limits.messages_per_min_chat
    };

    requests_sent_per_sec_count < limits.messages_per_sec_chat
        && requests_sent_per_min_count < messages_per_min_limit
}

// FIXME: https://github.com/rust-lang/rust-clippy/issues/11610
#[allow(clippy::needless_pass_by_ref_mut)]
async fn freeze(
    rx: &mut mpsc::Receiver<FreezeUntil>,
    mut slow_mode: Option<&mut HashMap<ChatIdHash, (Duration, Instant)>>,
    bot: &impl Requester,
    freezes: &mut Freezes,
    mut imm: Option<FreezeUntil>,
) {
    while let Some(freeze_until) = imm.take().or_else(|| rx.try_recv().ok()) {
        let FreezeUntil { until, after, chat } = freeze_until;

        // Clippy thinks that this `.as_deref_mut()` doesn't change the type (&mut
        // HashMap -> &mut HashMap), but it's actually a reborrow (the lifetimes
        // differ), since we are in a loop, simply using `slow_mode` would produce a
        // moved-out error.
        #[allow(clippy::needless_option_as_deref)]
        if let Some(slow_mode) = slow_mode.as_deref_mut() {
            // TODO: do something with channels?...
            if let hash @ ChatIdHash::Id(id) = chat {
                // TODO: maybe not call `get_chat` every time?

                // At this point there isn't much we can do with the error besides ignoring
                if let Ok(chat) = bot.get_chat(id).await {
                    match chat.slow_mode_delay() {
                        Some(delay) => {
                            let now = Instant::now();
                            let new_delay = delay.duration();
                            slow_mode.insert(hash, (new_delay, now));
                        }
                        None => {
                            slow_mode.remove(&hash);
                        }
                    };
                }
            }
        }

        // slow mode is enabled and it is <= to the delay asked by telegram
        let slow_mode_enabled_and_likely_the_cause = slow_mode
            .as_ref()
            .and_then(|m| m.get(&chat).map(|(delay, _)| delay <= &after))
            .unwrap_or(false);

        // Do not freeze if slow mode is enabled since the refusal is most likely caused
        // by the said slow mode and not by the limits.
        if !slow_mode_enabled_and_likely_the_cause {
            freezes.freeze(chat, until, after);
        }
    }
}

async fn read_from_rx(
    rx: &mut mpsc::Receiver<QueuedRequest>,
    backlog: &mut Backlog,
    intake: usize,
    rx_is_closed: &mut bool,
) {
    if backlog.is_empty() {
        log::debug!("blocking on queue");

        match rx.recv().await {
            Some(req) => backlog.push(req),
            None => *rx_is_closed = true,
        }
    }

    for _ in 0..intake {
        match rx.try_recv() {
            Ok(req) => backlog.push(req),
            Err(TryRecvError::Disconnected) => {
                *rx_is_closed = true;
                break;
            }
            // There are no items in queue.
            Err(TryRecvError::Empty) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tokio::{sync::mpsc, time::Instant};

    use super::{read_from_rx, worker, Backlog, FreezeUntil, InfoMessage, QueuedRequest};
    use crate::{
        adaptors::throttle::{
            request_lock::{channel, RequestWaiter},
            ChatIdHash, Limits, Settings, ThrottledChatKind,
        },
        types::ChatId,
        Bot,
    };

    const PRIVATE: ChatIdHash = ChatIdHash::Id(ChatId(1));
    const OTHER_PRIVATE: ChatIdHash = ChatIdHash::Id(ChatId(2));
    const SUPERGROUP: ChatIdHash = ChatIdHash::Id(ChatId(-1001234567890));

    #[tokio::test]
    async fn issue_535() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        // Close channel
        drop(tx);

        // Previously this caused an infinite loop
        read_from_rx(&mut rx, &mut Backlog::default(), 1, &mut false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn held_back_chat_does_not_block_others() {
        let limits = Limits { messages_per_min_chat: 2, ..Limits::default() };
        let tx = spawn_worker(Settings::default().limits(limits));

        let mut held_back = Vec::new();
        for _ in 0..40 {
            held_back.push(enqueue(&tx, PRIVATE).await);
        }
        let other = enqueue(&tx, OTHER_PRIVATE).await;

        let unlocked = tokio::time::timeout(Duration::from_secs(5), other).await;
        assert!(unlocked.is_ok(), "the other chat waited behind the held-back one");
    }

    #[tokio::test(start_paused = true)]
    async fn per_chat_order_is_kept() {
        let tx = spawn_worker(Settings::default());

        let first = enqueue(&tx, PRIVATE).await;
        let second = enqueue(&tx, PRIVATE).await;

        tokio::time::timeout(Duration::from_millis(500), first)
            .await
            .expect("the first request wasn't let through");
        let second = tokio::time::timeout(Duration::from_millis(500), second).await;
        assert!(second.is_err(), "the second request went within the same second");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_freezes_only_its_chat() {
        let tx = spawn_worker(Settings::default());

        let (_, freeze_tx) = enqueue(&tx, PRIVATE).await.await;
        let after = Duration::from_secs(30);
        let until = Instant::now() + after;
        freeze_tx.send(FreezeUntil { until, after, chat: PRIVATE }).await.unwrap();

        let frozen = enqueue(&tx, PRIVATE).await;
        let other = enqueue(&tx, OTHER_PRIVATE).await;

        let other = tokio::time::timeout(Duration::from_secs(2), other).await;
        assert!(other.is_ok(), "a chat that wasn't refused was frozen too");

        let mut frozen = Box::pin(frozen);
        let early = tokio::time::timeout(Duration::from_secs(20), &mut frozen).await;
        assert!(early.is_err(), "the refused chat was let through before its freeze ended");

        let late = tokio::time::timeout(Duration::from_secs(15), frozen).await;
        assert!(late.is_ok(), "the refused chat stayed frozen after its freeze ended");
    }

    #[tokio::test(start_paused = true)]
    async fn refusals_from_several_chats_freeze_everything() {
        let tx = spawn_worker(Settings::default().global_freeze_chats(2));

        let (_, freeze_tx) = enqueue(&tx, PRIVATE).await.await;
        let after = Duration::from_secs(30);
        let until = Instant::now() + after;
        freeze_tx.send(FreezeUntil { until, after, chat: PRIVATE }).await.unwrap();
        freeze_tx.send(FreezeUntil { until, after, chat: OTHER_PRIVATE }).await.unwrap();

        let third = enqueue(&tx, SUPERGROUP).await;
        let third = tokio::time::timeout(Duration::from_secs(20), third).await;
        assert!(third.is_err(), "a chat was let through while the bot was frozen");
    }

    #[tokio::test(start_paused = true)]
    async fn on_wait_reports_kind_and_wait() {
        let waits = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&waits);
        let settings = Settings::default()
            .on_wait(move |kind, waited| recorded.lock().unwrap().push((kind, waited)));
        let tx = spawn_worker(settings);

        let first = enqueue(&tx, SUPERGROUP).await;
        let second = enqueue(&tx, SUPERGROUP).await;
        first.await;
        second.await;

        let waits = waits.lock().unwrap();
        assert_eq!(waits.len(), 2);
        assert!(waits.iter().all(|(kind, _)| *kind == ThrottledChatKind::ChannelOrSupergroup));
        assert!(waits[1].1 >= Duration::from_secs(1), "the per-second limit cost no wait");
    }

    #[tokio::test(start_paused = true)]
    async fn limits_are_answered_while_idle() {
        let (_tx, rx) = mpsc::channel::<QueuedRequest>(1);
        let (info_tx, info_rx) = mpsc::channel(2);
        tokio::spawn(worker(Settings::default(), rx, info_rx, Bot::new("0:test")));

        let (response, limits) = tokio::sync::oneshot::channel();
        info_tx.send(InfoMessage::GetLimits { response }).await.unwrap();

        let limits = tokio::time::timeout(Duration::from_secs(1), limits).await;
        assert_eq!(limits.expect("an idle worker didn't answer").unwrap(), Limits::default());
    }

    fn spawn_worker(settings: Settings) -> mpsc::Sender<QueuedRequest> {
        let (tx, rx) = mpsc::channel(settings.limits.messages_per_sec_overall as usize);
        let (_info_tx, info_rx) = mpsc::channel(2);
        tokio::spawn(async move {
            worker(settings, rx, info_rx, Bot::new("0:test")).await
        });
        tx
    }

    async fn enqueue(tx: &mpsc::Sender<QueuedRequest>, chat: ChatIdHash) -> RequestWaiter {
        let (lock, waiter) = channel();
        tx.send(QueuedRequest { chat, lock, queued_at: Instant::now() }).await.unwrap();
        waiter
    }
}
