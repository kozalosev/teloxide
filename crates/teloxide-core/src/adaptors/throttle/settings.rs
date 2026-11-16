use std::{pin::Pin, time::Duration};

use futures::{future::ready, Future};

// Required to not trigger `clippy::type-complexity` lint
type BoxedFnMut<I, O> = Box<dyn FnMut(I) -> O + Send>;
type BoxedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type BoxedWaitFn = Box<dyn FnMut(ThrottledChatKind, Duration) + Send>;

/// Settings used by [`Throttle`] adaptor.
///
/// ## Examples
///
/// ```
/// use teloxide_core::adaptors::throttle;
///
/// let settings = throttle::Settings::default()
///     .on_queue_full(|pending| async move { /* do something when internal queue is full */ });
///
/// // use settings in `Throttle::with_settings` or other constructors
/// # let _ = settings;
/// ```
///
/// [`Throttle`]: crate::adaptors::throttle::Throttle
#[must_use]
#[non_exhaustive]
pub struct Settings {
    pub limits: Limits,
    /// Called when requests that are allowed to be sent to their chats are
    /// still waiting because the overall limit
    /// ([`Limits::messages_per_sec_overall`]) is used up. Gets the number of
    /// pending requests. Called at most once every 4 seconds.
    pub on_queue_full: BoxedFnMut<usize, BoxedFuture>,
    /// Called for every request the worker lets through, with the kind of its
    /// chat and the time it waited in the throttle.
    pub on_wait: BoxedWaitFn,
    pub retry: bool,
    pub check_slow_mode: bool,
    /// How many chats refused with `RetryAfter` at the same time freeze the
    /// whole bot rather than only themselves. A single refusal is about its
    /// own chat, but several at once suggest the bot-wide limit. `1` freezes
    /// the bot on every refusal, `0` never.
    pub global_freeze_chats: u32,
}

/// The kind of chat a throttled request is sent to.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ThrottledChatKind {
    Private,
    Group,
    ChannelOrSupergroup,
}

/// Telegram request limits.
///
/// This struct is used in [`Throttle`].
///
/// Note that you may ask telegram [@BotSupport] to increase limits for your
/// particular bot if it has a lot of users (but they may or may not do that).
///
/// [@BotSupport]: https://t.me/botsupport
/// [`Throttle`]: crate::adaptors::throttle::Throttle
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Limits {
    /// Allowed messages in one chat per second.
    pub messages_per_sec_chat: u32,

    /// Allowed messages in one chat per minute.
    pub messages_per_min_chat: u32,

    /// Allowed messages in one channel or supergroup per minute.
    pub messages_per_min_channel_or_supergroup: u32,

    /// Allowed messages per second.
    pub messages_per_sec_overall: u32,
}

impl Settings {
    pub fn limits(mut self, val: Limits) -> Self {
        self.limits = val;
        self
    }

    pub fn on_queue_full<F, Fut>(mut self, mut val: F) -> Self
    where
        F: FnMut(usize) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_queue_full = Box::new(move |pending| Box::pin(val(pending)));
        self
    }

    pub fn on_wait<F>(mut self, val: F) -> Self
    where
        F: FnMut(ThrottledChatKind, Duration) + Send + 'static,
    {
        self.on_wait = Box::new(val);
        self
    }

    pub fn global_freeze_chats(mut self, val: u32) -> Self {
        self.global_freeze_chats = val;
        self
    }

    pub fn no_retry(mut self) -> Self {
        self.retry = false;
        self
    }

    pub fn check_slow_mode(mut self) -> Self {
        self.check_slow_mode = true;
        self
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            limits: <_>::default(),
            on_queue_full: Box::new(|pending| {
                log::warn!("Throttle queue is full ({pending} pending requests)");
                Box::pin(ready(()))
            }),
            on_wait: Box::new(|_, _| {}),
            retry: true,
            check_slow_mode: false,
            global_freeze_chats: 3,
        }
    }
}

/// Defaults are taken from [telegram documentation][tgdoc] (except for
/// `messages_per_min_channel`).
///
/// [tgdoc]: https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
impl Default for Limits {
    fn default() -> Self {
        Self {
            messages_per_sec_chat: 1,
            messages_per_sec_overall: 30,
            messages_per_min_chat: 20,
            messages_per_min_channel_or_supergroup: 10,
        }
    }
}
