//! Observing the requests made to the Telegram Bot API.

use std::time::Duration;

use crate::errors::RequestError;

/// Observes every request made to the Telegram Bot API.
///
/// `teloxide` itself measures nothing and logs nothing about the requests it
/// sends. This trait is the hook for the code that wants to: the duration of a
/// request, the method it called, whether it failed and what was sent when it
/// did are all available here, and what to do with them — a histogram, a span,
/// a log record — is left to the implementor.
///
/// An observer is attached to a [`Bot`] with [`Bot::set_observer`] and is
/// called for every request that bot makes. Since [request adaptors] wrap a
/// [`Bot`], the observer sits *below* them and sees the actual HTTP call: the
/// time [`Throttle`] spends waiting in its queue is not counted as request
/// time.
///
/// [`Bot`]: crate::Bot
/// [`Bot::set_observer`]: crate::Bot::set_observer
/// [request adaptors]: crate::adaptors
/// [`Throttle`]: crate::adaptors::Throttle
///
/// ## Example
///
/// ```
/// use std::time::Duration;
///
/// use teloxide_core::{
///     observer::{RequestBody, RequestObservation, RequestObserver},
///     Bot, RequestError,
/// };
///
/// struct Log;
///
/// struct Started {
///     method: &'static str,
/// }
///
/// impl RequestObserver for Log {
///     fn on_request(
///         &self,
///         method: &'static str,
///         _body: RequestBody<'_>,
///     ) -> Box<dyn RequestObservation> {
///         Box::new(Started { method })
///     }
/// }
///
/// impl RequestObservation for Started {
///     fn on_response(self: Box<Self>, elapsed: Duration, outcome: Result<(), &RequestError>) {
///         let status = if outcome.is_ok() { "ok" } else { "failed" };
///         println!("{} {} in {:?}", self.method, status, elapsed);
///     }
/// }
///
/// let bot = Bot::new("TOKEN").set_observer(Log);
/// ```
pub trait RequestObserver: Send + Sync + 'static {
    /// Called just before the request is sent.
    ///
    /// `method` is the [`Payload::NAME`] of the request, e.g. `"SendMessage"`.
    /// Note that the payloads of the `..._inline` methods keep their `Inline`
    /// suffix here (`"EditMessageTextInline"`), even though the API method they
    /// call is the one without it.
    ///
    /// The returned observation is held across the request and consumed by
    /// [`RequestObservation::on_response`] once the outcome is known. It may
    /// own a [`tracing`] span guard, a timer, or anything else that has to live
    /// for the duration of the call.
    ///
    /// [`Payload::NAME`]: crate::requests::Payload::NAME
    /// [`tracing`]: https://docs.rs/tracing
    fn on_request(&self, method: &'static str, body: RequestBody<'_>) -> Box<dyn RequestObservation>;
}

/// The other half of a [`RequestObserver`], called when a request finishes.
///
/// See [`RequestObserver::on_request`].
pub trait RequestObservation: Send {
    /// Called once the request has finished, successfully or not.
    ///
    /// `elapsed` covers the whole call, including reading the response body.
    /// The successful case carries no value: reporting the deserialized
    /// response would make the trait generic over the output type and no longer
    /// object-safe.
    fn on_response(self: Box<Self>, elapsed: Duration, outcome: Result<(), &RequestError>);
}

/// The serialized request being sent, as passed to
/// [`RequestObserver::on_request`].
///
/// A JSON request carries its body, which is what makes it possible to log the
/// exact payload a failed request was sent with — an
/// [`ApiError::Unknown`](crate::ApiError::Unknown) rarely says which part of it
/// the API disliked. A multipart request (the file-uploading methods) is
/// already a stream by this point and cannot be shown without consuming it.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum RequestBody<'a> {
    /// The serialized JSON body of the request.
    Json(&'a [u8]),
    /// A multipart form, not available for inspection.
    Multipart,
}

impl<'a> RequestBody<'a> {
    /// Returns the JSON body, or `None` for a multipart request.
    #[must_use]
    pub fn json(self) -> Option<&'a [u8]> {
        match self {
            Self::Json(body) => Some(body),
            Self::Multipart => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{RequestBody, RequestObservation, RequestObserver};
    use crate::{requests::Requester, Bot, RequestError};

    /// What the observer saw, for the assertions to check.
    #[derive(Debug)]
    struct Observed {
        method: &'static str,
        body: Vec<u8>,
        failed: bool,
    }

    #[derive(Clone, Default)]
    struct Recorder {
        calls: Arc<Mutex<Vec<Observed>>>,
    }

    struct Pending {
        calls: Arc<Mutex<Vec<Observed>>>,
        method: &'static str,
        body: Vec<u8>,
    }

    impl RequestObserver for Recorder {
        fn on_request(&self, method: &'static str, body: RequestBody<'_>) -> Box<dyn RequestObservation> {
            Box::new(Pending {
                calls: Arc::clone(&self.calls),
                method,
                body: body.json().unwrap_or_default().to_owned(),
            })
        }
    }

    impl RequestObservation for Pending {
        fn on_response(
            self: Box<Self>,
            _elapsed: std::time::Duration,
            outcome: Result<(), &RequestError>,
        ) {
            let observed = Observed { method: self.method, body: self.body, failed: outcome.is_err() };
            self.calls.lock().unwrap().push(observed);
        }
    }

    /// A bot pointed at a port nothing listens on, so that the request fails
    /// without touching the network.
    fn unreachable_bot() -> Bot {
        let url = reqwest::Url::parse("http://127.0.0.1:1/").unwrap();
        Bot::new("TOKEN").set_api_url(url)
    }

    #[tokio::test]
    async fn observes_a_failed_request() {
        let recorder = Recorder::default();
        let bot = unreachable_bot().set_observer(recorder.clone());

        bot.get_me().await.expect_err("the request must fail");

        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].method, "GetMe");
        assert!(calls[0].failed);
    }

    #[tokio::test]
    async fn passes_the_serialized_payload() {
        let recorder = Recorder::default();
        let bot = unreachable_bot().set_observer(recorder.clone());

        bot.send_message(crate::types::ChatId(42), "hello")
            .await
            .expect_err("the request must fail");

        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls[0].method, "SendMessage");
        let body = String::from_utf8(calls[0].body.clone()).expect("the body must be valid UTF-8");
        assert!(body.contains("hello"), "the text is missing from the body: {body}");
    }

    #[tokio::test]
    async fn without_an_observer_nothing_is_recorded() {
        let recorder = Recorder::default();
        let bot = unreachable_bot();

        bot.get_me().await.expect_err("the request must fail");

        assert!(recorder.calls.lock().unwrap().is_empty());
    }
}
