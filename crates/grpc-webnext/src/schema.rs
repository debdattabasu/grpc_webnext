//! Descriptor source for `+json` termination.
//!
//! The proxy is schema-agnostic for binary `+proto` (it forwards opaque bytes), but
//! turning `+json` into the binary protobuf an upstream expects needs the message
//! descriptors. [`Schema`] resolves a [`Transcoder`] for a method path from one of
//! three sources; the binary path never touches it.
//!
//! Reflection is loaded **eagerly and whole**: on startup the proxy enumerates every
//! service the upstream exposes and builds a single transcoder covering all of them,
//! then refreshes it on a TTL (and on an operator-forced [`Schema::reload`]). One
//! consistent snapshot, rather than resolving each method lazily.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crate::{TranscodeError, Transcoder};
use tokio::sync::watch;
use tonic::transport::Channel;
use tonic::Status;

use crate::reflect;

/// How long a request will block waiting for the very first reflection load to land
/// (subsequent loads are swapped in atomically, so requests never wait again).
const FIRST_LOAD_WAIT: Duration = Duration::from_secs(10);
/// Upper bound on a single reflection enumeration, so a wedged upstream can't hang the
/// refresh task forever.
const LOAD_TIMEOUT: Duration = Duration::from_secs(30);
/// Retry cadence after a failed load (e.g. upstream not up yet), capped by the TTL.
const RETRY_INTERVAL: Duration = Duration::from_secs(30);

/// Where the proxy gets the descriptors it needs to transcode `+json`.
#[derive(Clone, Default)]
pub enum SchemaSource {
    /// Binary-only: `+json` is rejected with `Unimplemented` (the default).
    #[default]
    None,
    /// Fetch descriptors from the upstream's gRPC reflection service. The whole schema
    /// is enumerated eagerly and refreshed on a TTL (see `ProxyConfig::reflection_ttl`).
    Reflection,
    /// A precompiled `FileDescriptorSet` (`protoc --descriptor_set_out` / prost
    /// `file_descriptor_set_path`), for upstreams that don't expose reflection.
    Bundled(Bytes),
    /// Reflection primary, with the bundle as a fallback: the bundle serves immediately
    /// (and if the upstream has no reflection at all), and the live reflection snapshot
    /// takes over once it loads. Best of both — a baked-in safety net plus live
    /// descriptors when the upstream supports reflection.
    ReflectionOrBundled(Bytes),
}

/// Resolves a [`Transcoder`] for a gRPC method path according to a [`SchemaSource`].
/// Cheap to clone; the reflection snapshot is shared.
#[derive(Clone)]
pub struct Schema {
    inner: Inner,
}

#[derive(Clone)]
enum Inner {
    None,
    /// One transcoder covers every method in the bundle.
    Bundled(Arc<Transcoder>),
    Reflection(Arc<ReflectionState>),
    /// Reflection when its snapshot is loaded, else the bundle.
    ReflectionOrBundled { reflection: Arc<ReflectionState>, bundle: Arc<Transcoder> },
}

/// The live reflection snapshot plus what's needed to refresh it. The current
/// transcoder rides a `watch` channel: readers see the latest atomically, and a reader
/// that arrives before the first load blocks on it.
struct ReflectionState {
    channel: Channel,
    ttl: Duration,
    tx: watch::Sender<Option<Arc<Transcoder>>>,
    rx: watch::Receiver<Option<Arc<Transcoder>>>,
}

impl ReflectionState {
    /// Enumerate the upstream and atomically swap in the new snapshot.
    async fn load_and_swap(&self) -> Result<(), Status> {
        let tc = match tokio::time::timeout(LOAD_TIMEOUT, reflect::load_all(&self.channel)).await {
            Ok(result) => result?,
            Err(_) => return Err(Status::deadline_exceeded("upstream reflection load timed out")),
        };
        let _ = self.tx.send(Some(Arc::new(tc)));
        Ok(())
    }

    /// The current snapshot, or `None` if no load has landed yet (non-blocking).
    fn current(&self) -> Option<Arc<Transcoder>> {
        self.rx.borrow().clone()
    }

    /// The current transcoder, blocking (bounded) for the first load if none has landed.
    async fn transcoder(&self) -> Result<Arc<Transcoder>, Status> {
        let mut rx = self.rx.clone();
        loop {
            if let Some(tc) = rx.borrow_and_update().clone() {
                return Ok(tc);
            }
            if tokio::time::timeout(FIRST_LOAD_WAIT, rx.changed()).await.is_err() {
                return Err(Status::unavailable("descriptors not yet loaded from upstream reflection"));
            }
        }
    }
}

/// Background task: load once, then keep the snapshot fresh on the TTL. A failed load
/// keeps the previous snapshot (if any) and retries sooner than the TTL.
async fn refresh_loop(state: Arc<ReflectionState>) {
    loop {
        let wait = match state.load_and_swap().await {
            Ok(()) => {
                tracing::info!("proxy: loaded +json descriptors from upstream reflection");
                state.ttl
            }
            Err(e) => {
                tracing::warn!("proxy: reflection load failed ({e}); will retry");
                RETRY_INTERVAL.min(state.ttl)
            }
        };
        tokio::time::sleep(wait).await;
    }
}

impl Schema {
    /// Build a schema from an already-constructed transcoder — the in-process case, where
    /// the descriptors live in memory and there is no upstream to reflect against. `None`
    /// means no `+json` transcoder is configured.
    pub fn from_transcoder(transcoder: Option<Arc<Transcoder>>) -> Self {
        Self { inner: transcoder.map(Inner::Bundled).unwrap_or(Inner::None) }
    }

    /// Build a resolver. The `Bundled` set is parsed eagerly so a bad descriptor set
    /// fails at startup; `channel` is the upstream connection reused for reflection, and
    /// `ttl` is the reflection refresh interval. Call [`Schema::start`] afterwards to
    /// kick off the reflection loader.
    pub fn build(source: SchemaSource, channel: Channel, ttl: Duration) -> Result<Self, TranscodeError> {
        let inner = match source {
            SchemaSource::None => Inner::None,
            SchemaSource::Bundled(bytes) => {
                Inner::Bundled(Arc::new(Transcoder::from_file_descriptor_set(&bytes)?))
            }
            SchemaSource::Reflection => {
                let (tx, rx) = watch::channel(None);
                Inner::Reflection(Arc::new(ReflectionState { channel, ttl, tx, rx }))
            }
            SchemaSource::ReflectionOrBundled(bytes) => {
                let bundle = Arc::new(Transcoder::from_file_descriptor_set(&bytes)?);
                let (tx, rx) = watch::channel(None);
                Inner::ReflectionOrBundled {
                    reflection: Arc::new(ReflectionState { channel, ttl, tx, rx }),
                    bundle,
                }
            }
        };
        Ok(Self { inner })
    }

    /// Start the background reflection loader (eager initial load + TTL refresh). A
    /// no-op unless the source involves reflection. Must be called from within a Tokio
    /// runtime.
    pub fn start(&self) {
        let state = match &self.inner {
            Inner::Reflection(state) => state,
            Inner::ReflectionOrBundled { reflection, .. } => reflection,
            _ => return,
        };
        tokio::spawn(refresh_loop(state.clone()));
    }

    /// Force an immediate reflection reload, returning its result. The management hook
    /// behind `ProxyConfig::admin_reload_path`.
    pub async fn reload(&self) -> Result<(), Status> {
        match &self.inner {
            Inner::Reflection(state) | Inner::ReflectionOrBundled { reflection: state, .. } => {
                state.load_and_swap().await
            }
            Inner::None => Err(Status::failed_precondition("no descriptor source to reload")),
            Inner::Bundled(_) => {
                Err(Status::failed_precondition("bundled descriptors are static; nothing to reload"))
            }
        }
    }

    /// Whether this proxy can transcode `+json` at all.
    pub fn enabled(&self) -> bool {
        !matches!(self.inner, Inner::None)
    }

    /// The transcoder for this source, without validating any specific method — for
    /// REST annotation matching, where the target method isn't known until the URL is
    /// matched against the router.
    pub async fn transcoder_any(&self) -> Result<Arc<Transcoder>, Status> {
        match &self.inner {
            Inner::None => Err(Status::unimplemented(
                "no +json transcoder configured (pass a transcoder in-process, or enable upstream reflection / bundle a descriptor set on the proxy)",
            )),
            Inner::Bundled(tc) => Ok(tc.clone()),
            Inner::Reflection(state) => state.transcoder().await,
            // Prefer the live reflection snapshot; fall back to the bundle immediately
            // (no wait) while reflection loads or if the upstream has no reflection.
            Inner::ReflectionOrBundled { reflection, bundle } => {
                Ok(reflection.current().unwrap_or_else(|| bundle.clone()))
            }
        }
    }

    /// Resolve the transcoder for `method_path` (`/pkg.Service/Method`). An unknown
    /// method yields `Unimplemented` (uniformly across sources), distinct from a
    /// transcode failure.
    pub async fn transcoder(&self, method_path: &str) -> Result<Arc<Transcoder>, Status> {
        let tc = self.transcoder_any().await?;
        if tc.has_method(method_path) {
            Ok(tc)
        } else {
            Err(Status::unimplemented(format!("no descriptor for method {method_path}")))
        }
    }
}
