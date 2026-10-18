//! Blocking client socket transport for the headless server.
//!
//! This module owns the thin-client handshake, read loop, and writer loop.
//! It converts socket I/O into [`ServerEvent`] values consumed by
//! `HeadlessServer`.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SendError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use interprocess::local_socket::traits::Stream as _;
use interprocess::TryClone as _;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::ipc::LocalStream;
use crate::protocol::endpoint::{
    EndpointClientHello, EndpointServerWelcome, ENDPOINT_HELLO_KIND, ENDPOINT_PROTOCOL_GENERATION,
    ENDPOINT_WELCOME_KIND,
};
use crate::protocol::{
    self, AttachScrollDirection, AttachScrollSource, ClientMessage, ClientPaneInputEvent,
    RenderEncoding, ServerMessage, MAX_CLIPBOARD_IMAGE_PAYLOAD, MAX_FRAME_SIZE,
    MAX_GRAPHICS_FRAME_SIZE, PROTOCOL_VERSION,
};

/// Minimum accepted attached client size.
///
/// Narrow observers must be allowed to drive narrow renders, otherwise the
/// server wraps pane content against a wider width and the client sees the
/// right edge clipped.
const MIN_CLIENT_COLS: u16 = 1;
const MIN_CLIENT_ROWS: u16 = 1;

/// How long to wait for a client handshake before closing the connection.
/// Set to 4 seconds (rather than 5) to guarantee the connection is closed
/// within the 5-second deadline, even with OS timer slack, thread scheduling,
/// and cleanup overhead.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(4);

#[cfg(unix)]
const OBSERVER_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

// Bound the FIFO outside our replaceable queue too. A 4 KiB request (8 KiB
// reported by Linux) is smaller than one typical ~35 KiB presentation: at
// 16 KiB/s it adds about half a second, not many obsolete presentation items.
// This is not a frame-size limit. Large legal frames finish incrementally on
// the writer thread; no bytes are spliced, dropped, or timed out for being slow.
#[cfg(unix)]
const CLIENT_SEND_BUFFER_BYTES: libc::c_int = 4 * 1024;

/// Maximum input payload size (bytes) for a single `ClientMessage::Input`.
const MAX_INPUT_PAYLOAD: usize = 1024 * 1024; // 1 MB
const MAX_CLIENT_SHELL_DIMENSION: u16 = 4096;
const MAX_CLIENT_SHELL_CELLS: u32 = 1_000_000;
const MAX_CLIENT_CELL_SIZE_PX: u32 = 4096;

fn client_shell_geometry_error(
    surface_size: crate::protocol::ClientSurfaceSize,
    cell_width_px: u32,
    cell_height_px: u32,
) -> Option<&'static str> {
    if surface_size.cols == 0 || surface_size.rows == 0 {
        return Some("client shell requires a non-empty pane surface");
    }
    if surface_size.cols > MAX_CLIENT_SHELL_DIMENSION
        || surface_size.rows > MAX_CLIENT_SHELL_DIMENSION
        || u32::from(surface_size.cols) * u32::from(surface_size.rows) > MAX_CLIENT_SHELL_CELLS
    {
        return Some("client shell pane surface exceeds the safe geometry limit");
    }
    if cell_width_px > MAX_CLIENT_CELL_SIZE_PX || cell_height_px > MAX_CLIENT_CELL_SIZE_PX {
        return Some("client shell cell pixel size exceeds the safe geometry limit");
    }
    None
}

#[derive(serde::Deserialize)]
struct EndpointRequestHead {
    id: String,
    method: String,
}

enum DecodedEndpointRequest {
    Dispatch(Box<crate::api::schema::Request>),
    Error {
        request_id: String,
        code: &'static str,
        message: String,
    },
}

fn write_endpoint_rejection(stream: &mut LocalStream, code: &str, message: impl Into<String>) {
    let welcome = EndpointServerWelcome::incompatible(code, message);
    let response = ServerMessage::EndpointControl {
        kind: ENDPOINT_WELCOME_KIND.into(),
        data: serde_json::to_string(&welcome).unwrap_or_else(|_| "{}".into()),
    };
    let _ = protocol::write_message(stream, &response);
}

fn decode_endpoint_request(request: &str) -> serde_json::Result<DecodedEndpointRequest> {
    let head = serde_json::from_str::<EndpointRequestHead>(request)?;
    if !crate::server::client_commands::supports_client_shell_method_name(&head.method) {
        return Ok(DecodedEndpointRequest::Error {
            request_id: head.id,
            code: "unsupported_method",
            message: format!("method {:?} is not available on this machine", head.method),
        });
    }
    Ok(
        match serde_json::from_str::<crate::api::schema::Request>(request) {
            Ok(request) => DecodedEndpointRequest::Dispatch(Box::new(request)),
            Err(error) => DecodedEndpointRequest::Error {
                request_id: head.id,
                code: "invalid_request",
                message: format!("invalid endpoint request: {error}"),
            },
        },
    )
}
/// Maximum structured input events accepted in one client message.
const MAX_INPUT_EVENT_BATCH: usize = 4096;

/// Channels owned by the server side of a client writer thread.
#[derive(Clone, Debug)]
pub(crate) struct ClientWriter {
    /// Reliable control messages such as shutdown, notifications, and clipboard writes.
    pub(crate) control: ClientControlWriter,
    /// Droppable render messages. Capacity is one so slow clients cannot build lag.
    pub(crate) render: ClientRenderWriter,
}

impl ClientWriter {
    /// Drops only replaceable work, never an ordered graphics batch/fence.
    pub(crate) fn discard_pending_render(&self) -> bool {
        self.render.queue.discard_pending_render()
    }

    /// Cache a complete typed projection. Snapshot deltas are encoded at claim,
    /// against the last non-discardable snapshot, never a replaceable candidate.
    pub(crate) fn set_shell_projection(
        &self,
        frames: [Vec<u8>; 2],
        snapshot: Arc<protocol::ClientShellSnapshot>,
    ) -> Result<(), SendError<()>> {
        let mut state = self.render.queue.lock_state();
        if !state.writer_alive {
            return Err(SendError(()));
        }
        let [view, completions] = frames;
        state.projection = Some(Arc::new(ClientProjection {
            frames: [view, completions, Vec::new()],
            snapshot: Some(snapshot),
        }));
        Ok(())
    }

    /// Bootstrap controls are reliable and precede every presentation on this
    /// connection. They may therefore seed the ordered-stream snapshot base.
    pub(crate) fn seed_ordered_snapshot(&self, snapshot: Arc<protocol::ClientShellSnapshot>) {
        let mut state = self.render.queue.lock_state();
        if state.writer_alive {
            // Even an already-exhausted fairness burst cannot let a presentation
            // overtake the reliable full bootstrap it depends on.
            state.ordered_control_prefix = state.control.len();
            state.claimed_snapshot = Some(snapshot);
        }
    }

    #[cfg(test)]
    fn set_projection(&self, frames: [Vec<u8>; 3]) -> Result<(), SendError<()>> {
        let mut state = self.render.queue.lock_state();
        if !state.writer_alive {
            return Err(SendError(()));
        }
        state.projection = Some(Arc::new(ClientProjection { frames, snapshot: None }));
        Ok(())
    }

    pub(crate) fn send_projection(&self) -> Result<(), TrySendError<Vec<u8>>> {
        self.render.queue.send_projection()
    }

    #[cfg(all(test, unix))]
    pub(crate) fn test_paused() -> Self {
        let queue = ClientWriterQueue::new();
        Self {
            control: ClientControlWriter::queue(queue.clone()),
            render: ClientRenderWriter::queue(queue),
        }
    }

    #[cfg(all(test, unix))]
    pub(crate) fn test_paused_with_snapshot_delta() -> Self {
        let queue = ClientWriterQueue::new_with_snapshot_delta(true);
        Self {
            control: ClientControlWriter::queue(queue.clone()),
            render: ClientRenderWriter::queue(queue),
        }
    }

    #[cfg(all(test, unix))]
    pub(crate) fn test_drain(&self) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        loop {
            let claimed = {
                let mut state = self.render.queue.lock_state();
                self.render.queue.pop(&mut state)
            };
            let Some(claimed) = claimed else { break };
            let item = self.render.queue.materialize(claimed).expect("encode claimed test projection");
            match item {
                ClientWriteItem::Control(data) | ClientWriteItem::Render(data) => frames.push(data),
                ClientWriteItem::Presentation(projection, data, snapshot_frame) => {
                    frames.extend(projection.iter().filter(|frame| !frame.is_empty()).cloned());
                    if !snapshot_frame.is_empty() {
                        frames.push(snapshot_frame);
                    }
                    if !data.is_empty() {
                        frames.push(data);
                    }
                }
            }
        }
        frames
    }

    #[cfg(test)]
    pub(crate) fn test_fill_render(&self, data: Vec<u8>) {
        self.render.try_send(data).unwrap();
    }

    #[cfg(test)]
    pub(crate) fn test_close(&self) {
        self.render.queue.close_writer();
    }

    #[cfg(test)]
    pub(crate) fn test_channel(
        control: std::sync::mpsc::Sender<Vec<u8>>,
        render: std::sync::mpsc::SyncSender<Vec<u8>>,
    ) -> Self {
        let queue = ClientWriterQueue::new();
        let drain = queue.clone();
        let control_writer = ClientControlWriter::queue(queue.clone());
        let render_writer = ClientRenderWriter::queue(queue);
        let writer = Self {
            control: control_writer,
            render: render_writer,
        };
        std::thread::spawn(move || {
            while let Some(item) = drain.recv() {
                let sent = match item {
                    ClientWriteItem::Control(data) => control.send(data).is_ok(),
                    ClientWriteItem::Render(data) => render.send(data).is_ok(),
                    ClientWriteItem::Presentation(projection, data, snapshot_frame) => {
                        projection.iter().filter(|frame| !frame.is_empty())
                            .all(|frame| control.send(frame.clone()).is_ok())
                            && (snapshot_frame.is_empty() || control.send(snapshot_frame).is_ok())
                            && (data.is_empty() || render.send(data).is_ok())
                    }
                };
                if !sent {
                    break;
                }
            }
            drain.close_writer();
        });
        writer
    }
}

#[derive(Debug)]
pub(crate) struct ClientControlWriter {
    queue: Arc<ClientWriterQueue>,
}

#[derive(Debug)]
pub(crate) struct ClientRenderWriter {
    queue: Arc<ClientWriterQueue>,
}

macro_rules! writer_handle {
    ($type:ty) => {
        impl Clone for $type {
            fn clone(&self) -> Self {
                self.queue.add_sender();
                Self {
                    queue: self.queue.clone(),
                }
            }
        }
        impl Drop for $type {
            fn drop(&mut self) {
                self.queue.remove_sender();
            }
        }
    };
}
writer_handle!(ClientControlWriter);
writer_handle!(ClientRenderWriter);

impl ClientControlWriter {
    fn queue(queue: Arc<ClientWriterQueue>) -> Self {
        queue.add_sender();
        Self {
            queue,
        }
    }

    pub(crate) fn send(&self, data: Vec<u8>) -> Result<(), SendError<Vec<u8>>> {
        self.queue.send_control(data)
    }
}

impl ClientRenderWriter {
    fn queue(queue: Arc<ClientWriterQueue>) -> Self {
        queue.add_sender();
        Self {
            queue,
        }
    }

    #[cfg(test)]
    pub(crate) fn try_send(&self, data: Vec<u8>) -> Result<(), TrySendError<Vec<u8>>> {
        self.queue.try_send_render(data)
    }

    pub(crate) fn try_send_tracked(
        &self,
        data: Vec<u8>,
    ) -> Result<Arc<AtomicBool>, TrySendError<Vec<u8>>> {
        self.queue.enqueue_render_tracked(data)
    }

    /// Graphics delivery caches advance on enqueue; do not coalesce their work.
    pub(crate) fn try_send_protected(&self, data: Vec<u8>) -> Result<(), TrySendError<Vec<u8>>> {
        self.queue.enqueue_render(data, true)
    }

    pub(crate) fn send_ordered(&self, data: Vec<u8>) -> Result<(), TrySendError<Vec<u8>>> {
        self.queue.send_ordered(data)
    }
}

// Two maximum graphics-sized framed messages leave room for a legal 16 MiB
// clipboard image plus a normal RPC/notification burst. Tiny messages also have
// an independent cap; neither budget includes the single in-flight write.
const MAX_PENDING_CONTROL_BYTES: usize = 2 * (MAX_GRAPHICS_FRAME_SIZE + 4);
const MAX_PENDING_CONTROL_ITEMS: usize = 1024;
const CONTROL_BURST: usize = 16;

#[derive(Debug, PartialEq, Eq)]
struct ClientProjection {
    frames: [Vec<u8>; 3],
    snapshot: Option<Arc<protocol::ClientShellSnapshot>>,
}

impl ClientProjection {
    fn iter(&self) -> std::slice::Iter<'_, Vec<u8>> {
        self.frames.iter()
    }
}

impl std::ops::Index<usize> for ClientProjection {
    type Output = Vec<u8>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.frames[index]
    }
}

type ProjectionFrames = Arc<ClientProjection>;

#[derive(Debug)]
struct ClientWriterQueue {
    state: Mutex<ClientWriterQueueState>,
    ready: Condvar,
    disconnect: Mutex<Option<LocalStream>>,
    snapshot_delta: bool,
    #[cfg(test)]
    projection_encode_gate: std::sync::OnceLock<(
        std::sync::mpsc::Sender<()>,
        Arc<std::sync::Barrier>,
        AtomicBool,
    )>,
}

#[derive(Debug, Default)]
struct ClientWriterQueueState {
    control: VecDeque<Vec<u8>>,
    control_bytes: usize,
    control_burst: usize,
    ordered_control_prefix: usize,
    ordered: VecDeque<ClientWriteItem>,
    render: Option<ClientWriteItem>,
    render_protected: bool,
    render_claim: Option<Arc<AtomicBool>>,
    projection: Option<ProjectionFrames>,
    claimed_projection: Option<ProjectionFrames>,
    claimed_snapshot: Option<Arc<protocol::ClientShellSnapshot>>,
    senders: usize,
    writer_alive: bool,
}

impl ClientWriterQueueState {
    fn presentation(&self, data: Vec<u8>) -> ClientWriteItem {
        match &self.projection {
            Some(projection) => ClientWriteItem::Presentation(projection.clone(), data, Vec::new()),
            None => ClientWriteItem::Render(data),
        }
    }

    fn clear(&mut self) {
        // Release both payloads and the VecDeque storage on disconnection.
        self.control = VecDeque::new();
        self.control_bytes = 0;
        self.ordered_control_prefix = 0;
        self.ordered = VecDeque::new();
        self.render = None;
        self.render_claim = None;
        self.projection = None;
        self.claimed_projection = None;
        self.claimed_snapshot = None;
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ClientWriteItem {
    Control(Vec<u8>),
    Render(Vec<u8>),
    /// Shared immutable companions, surface bytes, separately owned encoded snapshot.
    Presentation(ProjectionFrames, Vec<u8>, Vec<u8>),
}

/// Immutable work captured at claim. Encoding this owned value must happen
/// outside the queue mutex, so large inventories never block producers.
enum ClaimedWriteItem {
    Ready(ClientWriteItem),
    Projection {
        projection: ProjectionFrames,
        base: Option<Arc<protocol::ClientShellSnapshot>>,
        allow_delta: bool,
        surface: Vec<u8>,
    },
}

impl ClaimedWriteItem {
    fn materialize(self) -> io::Result<ClientWriteItem> {
        let (projection, base, allow_delta, surface) = match self {
            Self::Ready(item) => return Ok(item),
            Self::Projection { projection, base, allow_delta, surface } =>
                (projection, base, allow_delta, surface),
        };
        let snapshot = projection.snapshot.as_deref().expect("claimed typed projection");
        let message = protocol::endpoint::snapshot_message(snapshot, base.as_deref(), allow_delta)
            .map_err(io::Error::other)?;
        let mut snapshot_frame = Vec::new();
        protocol::write_message(&mut snapshot_frame, &message).map_err(io::Error::other)?;
        Ok(ClientWriteItem::Presentation(projection, surface, snapshot_frame))
    }
}

impl ClientWriterQueue {
    #[cfg(test)]
    fn new() -> Arc<Self> {
        Self::new_with_snapshot_delta(false)
    }

    fn new_with_snapshot_delta(snapshot_delta: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ClientWriterQueueState {
                writer_alive: true,
                ..ClientWriterQueueState::default()
            }),
            ready: Condvar::new(),
            disconnect: Mutex::new(None),
            snapshot_delta,
            #[cfg(test)]
            projection_encode_gate: std::sync::OnceLock::new(),
        })
    }

    fn set_disconnect_stream(&self, stream: LocalStream) {
        *self.disconnect.lock().unwrap_or_else(|p| p.into_inner()) = Some(stream);
    }

    fn disconnect(&self) {
        if let Some(stream) = self.disconnect.lock().unwrap_or_else(|p| p.into_inner()).take() {
            #[cfg(unix)]
            let _ = crate::platform::shutdown_client_stream(&stream);
            #[cfg(windows)]
            {
                use std::os::windows::io::AsRawHandle as _;
                let LocalStream::NamedPipe(pipe) = &stream;
                // These are server-side pipe handles. Disconnect wakes synchronous
                // reads/writes on all clones, unlike merely dropping this clone.
                unsafe {
                    windows_sys::Win32::System::Pipes::DisconnectNamedPipe(pipe.as_raw_handle());
                }
            }
        }
    }

    fn add_sender(&self) {
        let mut state = self.lock_state();
        state.senders = state.senders.saturating_add(1);
    }

    fn remove_sender(&self) {
        let mut state = self.lock_state();
        state.senders = state.senders.saturating_sub(1);
        self.ready.notify_one();
    }

    fn send_control(&self, data: Vec<u8>) -> Result<(), SendError<Vec<u8>>> {
        let mut state = self.lock_state();
        if !state.writer_alive {
            return Err(SendError(data));
        }
        if state.control.len() >= MAX_PENDING_CONTROL_ITEMS
            || data.len() > MAX_PENDING_CONTROL_BYTES.saturating_sub(state.control_bytes)
        {
            warn!("client reliable output budget exceeded; disconnecting slow client");
            state.writer_alive = false;
            state.clear();
            self.ready.notify_all();
            drop(state);
            self.disconnect();
            return Err(SendError(data));
        }
        state.control_bytes += data.len();
        state.control.push_back(data);
        self.ready.notify_one();
        Ok(())
    }

    #[cfg(test)]
    fn try_send_render(&self, data: Vec<u8>) -> Result<(), TrySendError<Vec<u8>>> {
        self.enqueue_render(data, false)
    }

    fn enqueue_render(&self, data: Vec<u8>, protected: bool) -> Result<(), TrySendError<Vec<u8>>> {
        let mut state = self.lock_state();
        if !state.writer_alive {
            return Err(TrySendError::Disconnected(data));
        }
        if state.render.is_some() {
            return Err(TrySendError::Full(data));
        }
        state.render = Some(state.presentation(data));
        state.render_protected = protected;
        self.ready.notify_one();
        Ok(())
    }

    fn enqueue_render_tracked(
        &self,
        data: Vec<u8>,
    ) -> Result<Arc<AtomicBool>, TrySendError<Vec<u8>>> {
        let mut state = self.lock_state();
        if !state.writer_alive {
            return Err(TrySendError::Disconnected(data));
        }
        if state.render.is_some() {
            return Err(TrySendError::Full(data));
        }
        let claimed = Arc::new(AtomicBool::new(false));
        state.render = Some(state.presentation(data));
        state.render_protected = false;
        state.render_claim = Some(claimed.clone());
        self.ready.notify_one();
        Ok(claimed)
    }

    fn send_projection(&self) -> Result<(), TrySendError<Vec<u8>>> {
        let mut state = self.lock_state();
        if !state.writer_alive {
            return Err(TrySendError::Disconnected(Vec::new()));
        }
        // Metadata-only clients may replace metadata-only work atomically. Never
        // drop a surface here: only its app owner can invalidate the delta base.
        if state.render.as_ref().is_some_and(|item| {
            matches!(item, ClientWriteItem::Presentation(_, surface, _) if surface.is_empty())
        }) {
            state.render = None;
        }
        if state.render.is_some() {
            return Err(TrySendError::Full(Vec::new()));
        }
        state.render = Some(state.presentation(Vec::new()));
        state.render_protected = false;
        self.ready.notify_one();
        Ok(())
    }

    fn discard_pending_render(&self) -> bool {
        let mut state = self.lock_state();
        if state.render_protected {
            return false;
        }
        state.render_claim = None;
        state.render.take().is_some()
    }

    fn send_ordered(&self, data: Vec<u8>) -> Result<(), TrySendError<Vec<u8>>> {
        let mut state = self.lock_state();
        if !state.writer_alive {
            return Err(TrySendError::Disconnected(data));
        }
        if !state.ordered.is_empty() {
            return Err(TrySendError::Full(data));
        }
        if let Some(older) = state.render.take() {
            if let Some(claimed) = state.render_claim.take() {
                claimed.store(true, Ordering::Release);
            }
            state.ordered.push_back(older);
        }
        let batch = state.presentation(data);
        state.ordered.push_back(batch);
        self.ready.notify_one();
        Ok(())
    }

    fn pop(&self, state: &mut ClientWriterQueueState) -> Option<ClaimedWriteItem> {
        if !state.writer_alive {
            return None;
        }
        // Reliable FIFO is preserved. Text presentations get bounded fairness;
        // graphics retain control priority so retirement always precedes a queued
        // file upload (the export may already have been unlinked).
        if state.control_burst < CONTROL_BURST
            || state.ordered_control_prefix != 0
            || !state.ordered.is_empty()
            || state.render_protected
            || state.render.is_none()
        {
            if let Some(data) = state.control.pop_front() {
                state.control_bytes -= data.len();
                state.ordered_control_prefix = state.ordered_control_prefix.saturating_sub(1);
                state.control_burst = state.control_burst.saturating_add(1);
                return Some(ClaimedWriteItem::Ready(ClientWriteItem::Control(data)));
            }
        }
        let item = state.ordered.pop_front().or_else(|| {
            let item = state.render.take();
            if let Some(claimed) = state.render_claim.take() {
                claimed.store(true, Ordering::Release);
            }
            item
        });
        if let Some(item) = item {
            state.control_burst = 0;
            self.ready.notify_one();
            return Some(match item {
                ClientWriteItem::Presentation(projection, data, snapshot_frame) => {
                    let already_claimed = state.claimed_projection.as_ref()
                        .is_some_and(|previous| Arc::ptr_eq(previous, &projection));
                    if already_claimed {
                        ClaimedWriteItem::Ready(ClientWriteItem::Render(data))
                    } else {
                        let claimed = if let Some(snapshot) = &projection.snapshot {
                            let base = state.claimed_snapshot.replace(snapshot.clone());
                            ClaimedWriteItem::Projection {
                                projection: projection.clone(),
                                base,
                                allow_delta: self.snapshot_delta,
                                surface: data,
                            }
                        } else {
                            ClaimedWriteItem::Ready(ClientWriteItem::Presentation(projection.clone(), data, snapshot_frame))
                        };
                        state.claimed_projection = Some(projection);
                        claimed
                    }
                }
                item => ClaimedWriteItem::Ready(item),
            });
        }
        None
    }

    fn recv(&self) -> Option<ClientWriteItem> {
        let mut state = self.lock_state();
        loop {
            if let Some(item) = self.pop(&mut state) {
                drop(state);
                return self.materialize(item);
            }
            if !state.writer_alive || state.senders == 0 {
                return None;
            }
            state = self.ready.wait(state).unwrap_or_else(|p| p.into_inner());
        }
    }

    fn materialize(&self, claimed: ClaimedWriteItem) -> Option<ClientWriteItem> {
        #[cfg(test)]
        if matches!(&claimed, ClaimedWriteItem::Projection { .. }) {
            if let Some((entered, resume, used)) = self.projection_encode_gate.get() {
                if !used.swap(true, Ordering::AcqRel) {
                    let _ = entered.send(());
                    resume.wait();
                }
            }
        }
        match claimed.materialize() {
            Ok(item) => Some(item),
            Err(error) => {
                warn!(%error, "failed to encode claimed shell projection");
                self.close_writer();
                None
            }
        }
    }

    fn close_writer(&self) {
        let mut state = self.lock_state();
        state.writer_alive = false;
        state.clear();
        self.ready.notify_all();
        drop(state);
        self.disconnect();
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ClientWriterQueueState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// Internal event sent from client transport threads to the main event loop.
#[derive(Debug)]
pub(crate) enum ServerEvent {
    /// A new client completed the handshake.
    ClientConnected {
        client_id: u64,
        cols: u16,
        rows: u16,
        cell_width_px: u32,
        cell_height_px: u32,
        pixel_mouse: bool,
        writer: ClientWriter,
    },
    /// A client-owned shell completed its dedicated handshake.
    ClientShellConnected {
        client_id: u64,
        surface_cols: u16,
        surface_rows: u16,
        cell_width_px: u32,
        cell_height_px: u32,
        pixel_mouse: bool,
        direct_graphics: bool,
        endpoint_keybindings: bool,
        mouse_capture: bool,
        surface_active: bool,
        surface_reuse: bool,
        surface_delta: bool,
        surface_scroll: bool,
        writer: ClientWriter,
    },
    /// A client sent an input message.
    ClientInput { client_id: u64, data: Vec<u8> },
    /// A client reported the one armed Kitty regular-file response.
    GraphicsTransmissionResult {
        client_id: u64,
        transfer_id: u64,
        image_id: u32,
        success: bool,
    },
    GraphicsTransmissionStarted {
        client_id: u64,
        transfer_id: u64,
        image_id: u32,
    },
    /// A fully decoded interactive paste exceeded the text-input limit.
    ClientPasteRejected {
        client_id: u64,
        size: usize,
        max: usize,
    },
    /// A client sent local clipboard image bytes to paste into a remote pane.
    ClientClipboardImage {
        client_id: u64,
        target: crate::protocol::ClientClipboardImageTarget,
        extension: String,
        data: Vec<u8>,
    },
    /// A client requested direct attach to one terminal.
    ClientAttachTerminal {
        client_id: u64,
        terminal_id: String,
        takeover: bool,
    },
    /// A client requested read-only observation of one terminal.
    ClientObserveTerminal { client_id: u64, target: String },
    /// A client requested writable control of one terminal.
    ClientControlTerminal {
        client_id: u64,
        target: String,
        takeover: bool,
    },
    /// A direct terminal attach client requested scrollback movement.
    ClientAttachScroll {
        client_id: u64,
        source: AttachScrollSource,
        direction: AttachScrollDirection,
        lines: u16,
        column: Option<u16>,
        row: Option<u16>,
        modifiers: u8,
    },
    /// A direct terminal attach client delivered one structured mouse event.
    ClientAttachMouse {
        client_id: u64,
        kind: crate::protocol::ClientMouseKind,
        position: crate::protocol::ClientMousePosition,
        geometry: Option<crate::protocol::ClientMouseGeometry>,
        modifiers: u8,
        lines: u16,
    },
    /// A client sent a resize message.
    ClientResize {
        client_id: u64,
        cols: u16,
        rows: u16,
        cell_width_px: u32,
        cell_height_px: u32,
        pixel_mouse: bool,
    },
    /// A client-owned shell recomputed its pane viewport.
    ClientShellResize {
        client_id: u64,
        surface_cols: u16,
        surface_rows: u16,
        cell_width_px: u32,
        cell_height_px: u32,
        pixel_mouse: bool,
    },
    /// A client-owned shell delivered semantic input to one stable pane target.
    ClientShellPaneInput {
        client_id: u64,
        pane_id: String,
        events: Vec<ClientPaneInputEvent>,
    },
    /// A client-owned shell delivered semantic input to its active popup terminal.
    ClientShellPopupInput {
        client_id: u64,
        terminal_id: String,
        events: Vec<ClientPaneInputEvent>,
    },
    /// A client-owned shell published one host terminal theme observation.
    ClientShellHostTheme {
        client_id: u64,
        update: crate::protocol::ClientHostThemeUpdate,
    },
    /// A client-owned shell reported whether its outer terminal has focus.
    ClientShellFocus { client_id: u64, focused: bool },
    /// A client-owned shell updated its local mouse-capture preference.
    ClientShellMouseCapture { client_id: u64, enabled: bool },
    /// The committed shell asks the server to replay presentation effects before input resumes.
    ClientShellPresentationSync { client_id: u64, token: String },
    /// A client-owned shell invoked one endpoint operation through this connection.
    ClientShellEndpointRequest {
        client_id: u64,
        boot_id: String,
        request: Box<crate::api::schema::Request>,
    },
    /// A well-framed endpoint request could not be dispatched by this server.
    ClientShellEndpointRequestError {
        client_id: u64,
        boot_id: String,
        request_id: String,
        code: &'static str,
        message: String,
    },
    /// One chunk of a deferred endpoint operation's final response is ready.
    ClientShellEndpointResponseChunkReady {
        client_id: u64,
        boot_id: String,
        request_id: String,
        final_chunk: bool,
        data: Vec<u8>,
    },
    /// A client detached gracefully.
    ClientDetach { client_id: u64 },
    /// A client connection was lost.
    ClientDisconnected { client_id: u64 },
    /// A client writer drained its render slot and can accept another render.
    ClientWriterDrained { client_id: u64 },
    /// Ctrl+C or external shutdown signal received.
    QuitSignal,
}

/// Clamp client-reported terminal dimensions to a minimum viable size.
pub(crate) fn clamp_terminal_size(cols: u16, rows: u16) -> (u16, u16) {
    let clamped_cols = cols.max(MIN_CLIENT_COLS);
    let clamped_rows = rows.max(MIN_CLIENT_ROWS);
    (clamped_cols, clamped_rows)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputEventLimit {
    WithinLimits,
    TooManyEvents,
    PasteTooLarge { size: usize },
    InputPayloadTooLarge { size: usize },
}

fn pane_input_event_limit(events: &[ClientPaneInputEvent]) -> InputEventLimit {
    let mut expanded_events = 0usize;
    let mut paste_bytes = 0usize;
    let mut input_bytes = 0usize;
    for event in events {
        expanded_events = expanded_events.saturating_add(match event {
            ClientPaneInputEvent::Key { repeat_count, .. } => usize::from((*repeat_count).max(1)),
            ClientPaneInputEvent::Mouse {
                kind:
                    crate::protocol::ClientMouseKind::ScrollUp
                    | crate::protocol::ClientMouseKind::ScrollDown,
                lines,
                ..
            } => usize::from((*lines).max(1)),
            ClientPaneInputEvent::TextCommit(_)
            | ClientPaneInputEvent::Mouse { .. }
            | ClientPaneInputEvent::Paste(_) => 1,
        });
        match event {
            ClientPaneInputEvent::Key {
                repeat_count,
                generated_text,
                ..
            } => {
                if let Some(text) = generated_text {
                    input_bytes = input_bytes.saturating_add(
                        text.len()
                            .saturating_mul(usize::from((*repeat_count).max(1))),
                    );
                }
            }
            ClientPaneInputEvent::TextCommit(text) => {
                input_bytes = input_bytes.saturating_add(text.len());
            }
            ClientPaneInputEvent::Mouse { .. } => {}
            ClientPaneInputEvent::Paste(text) => {
                paste_bytes = paste_bytes.saturating_add(text.len());
            }
        }
    }

    classify_input_event_size(expanded_events, paste_bytes, input_bytes)
}

fn classify_input_event_size(
    expanded_events: usize,
    paste_bytes: usize,
    input_bytes: usize,
) -> InputEventLimit {
    if expanded_events > MAX_INPUT_EVENT_BATCH {
        return InputEventLimit::TooManyEvents;
    }

    let payload_bytes = paste_bytes.saturating_add(input_bytes);
    if payload_bytes <= MAX_INPUT_PAYLOAD {
        InputEventLimit::WithinLimits
    } else if input_bytes == 0 {
        InputEventLimit::PasteTooLarge {
            size: payload_bytes,
        }
    } else {
        InputEventLimit::InputPayloadTooLarge {
            size: payload_bytes,
        }
    }
}

#[cfg(windows)]
fn set_client_recv_timeout(
    stream: &LocalStream,
    timeout: Option<Duration>,
    context: &'static str,
    client_id: u64,
) -> io::Result<()> {
    match stream.set_recv_timeout(timeout) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::Unsupported => {
            debug!(client_id, err = %err, context, "client socket receive timeout unavailable");
            Ok(())
        }
        Err(err) => Err(err),
    }
}

#[cfg(not(windows))]
fn set_client_recv_timeout(
    stream: &LocalStream,
    timeout: Option<Duration>,
    _context: &'static str,
    _client_id: u64,
) -> io::Result<()> {
    stream.set_recv_timeout(timeout)
}

#[cfg(unix)]
fn configure_client_write_buffer(stream: &LocalStream) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    let LocalStream::UdSocket(socket) = stream;
    let bytes = CLIENT_SEND_BUFFER_BYTES;
    let result = unsafe {
        libc::setsockopt(
            socket.inner().as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            (&bytes as *const libc::c_int).cast(),
            std::mem::size_of_val(&bytes) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Handles the client handshake on a blocking thread.
///
/// Reads the `TerminalHello` or `ClientShellHello` message, validates the version,
/// sends `Welcome`, and then enters a read loop forwarding messages to the server event channel.
pub(crate) fn handle_client_handshake(
    mut stream: LocalStream,
    client_id: u64,
    server_event_tx: &mpsc::Sender<ServerEvent>,
    should_quit: &Arc<AtomicBool>,
) -> io::Result<()> {
    if should_quit.load(Ordering::Acquire) {
        return Ok(());
    }

    // Reset to blocking mode — the accept loop sets nonblocking but
    // the handshake thread needs blocking I/O for read_message/write_message.
    stream.set_nonblocking(false)?;

    set_client_recv_timeout(
        &stream,
        Some(HANDSHAKE_TIMEOUT),
        "client handshake read timeout unavailable",
        client_id,
    )?;

    // Read the handshake message.
    let hello: ClientMessage = match protocol::read_message(&mut stream, MAX_FRAME_SIZE) {
        Ok(msg) => msg,
        Err(protocol::FramingError::UnexpectedEof) => {
            debug!(client_id, "client disconnected before handshake");
            return Ok(());
        }
        Err(protocol::FramingError::Oversized { claimed, max }) => {
            warn!(client_id, claimed, max, "oversized handshake from client");
            return Ok(());
        }
        Err(err) => {
            debug!(client_id, err = %err, "failed to read client hello");
            return Ok(());
        }
    };

    let mut endpoint_welcome = None;
    let mut snapshot_delta = false;
    let (
        client_cols,
        client_rows,
        cell_width_px,
        cell_height_px,
        terminal_pixel_mouse,
        shell_options,
    ) = match hello {
        ClientMessage::TerminalHello {
            version,
            cols,
            rows,
            cell_width_px,
            cell_height_px,
            pixel_mouse,
        } => {
            if let protocol::VersionCheck::Incompatible(reason) =
                protocol::check_client_version(version)
            {
                let welcome = ServerMessage::Welcome {
                    version: PROTOCOL_VERSION,
                    encoding: RenderEncoding::TerminalAnsi,
                    error: Some(reason),
                };
                let _ = protocol::write_message(&mut stream, &welcome);
                return Ok(());
            }
            let (cols, rows) = clamp_terminal_size(cols, rows);
            (cols, rows, cell_width_px, cell_height_px, pixel_mouse, None)
        }
        ClientMessage::EndpointControl { kind, data } if kind == ENDPOINT_HELLO_KIND => {
            let hello: EndpointClientHello = match serde_json::from_str(&data) {
                Ok(hello) => hello,
                Err(error) => {
                    write_endpoint_rejection(
                        &mut stream,
                        "invalid_hello",
                        format!("invalid endpoint hello: {error}"),
                    );
                    return Ok(());
                }
            };
            let incompatibility = if hello.generation != ENDPOINT_PROTOCOL_GENERATION {
                Some((
                    "unsupported_generation",
                    format!(
                        "endpoint generation {} is unsupported; this server supports generation {ENDPOINT_PROTOCOL_GENERATION}",
                        hello.generation
                    ),
                ))
            } else if !hello.supports_required_codecs() {
                Some((
                    "no_common_core",
                    "client and server have no compatible endpoint core codecs".to_owned(),
                ))
            } else {
                client_shell_geometry_error(
                    hello.surface_size,
                    hello.cell_width_px,
                    hello.cell_height_px,
                )
                .map(|reason| ("invalid_surface", reason.to_owned()))
            };
            if let Some((code, reason)) = incompatibility {
                write_endpoint_rejection(&mut stream, code, reason);
                return Ok(());
            }
            snapshot_delta = hello.supports_snapshot_delta();
            endpoint_welcome = Some(EndpointServerWelcome::compatible_for_hello(
                crate::server::client_commands::supported_client_shell_method_names()
                    .iter().map(|method| (*method).to_owned()).collect(),
                &hello,
            ));
            (
                hello.surface_size.cols,
                hello.surface_size.rows,
                hello.cell_width_px,
                hello.cell_height_px,
                false,
                Some((
                    hello.pixel_mouse,
                    hello.direct_graphics,
                    hello.endpoint_keybindings,
                    hello.mouse_capture,
                    hello.surface_active,
                    hello.surface_reuse,
                    hello.surface_delta,
                    hello.surface_scroll,
                )),
            )
        }
        ClientMessage::ClientShellHello { .. } => {
            let welcome = ServerMessage::Welcome {
                version: PROTOCOL_VERSION,
                encoding: RenderEncoding::SemanticFrame,
                error: Some(
                    "this client predates the stable endpoint protocol; upgrade the Herdr client"
                        .to_owned(),
                ),
            };
            let _ = protocol::write_message(&mut stream, &welcome);
            return Ok(());
        }
        _ => {
            debug!(client_id, "first message was not a handshake, closing");
            let welcome = ServerMessage::Welcome {
                version: PROTOCOL_VERSION,
                encoding: RenderEncoding::SemanticFrame,
                error: Some(
                    "expected TerminalHello or ClientShellHello as first message".to_owned(),
                ),
            };
            let _ = protocol::write_message(&mut stream, &welcome);
            return Ok(());
        }
    };

    if should_quit.load(Ordering::Acquire) {
        return Ok(());
    }

    // Send the negotiated welcome. Endpoint compatibility is independent from
    // the same-install protocol used by direct terminal clients.
    let render_encoding = if shell_options.is_some() {
        RenderEncoding::SemanticFrame
    } else {
        RenderEncoding::TerminalAnsi
    };
    let welcome = if let Some(welcome) = endpoint_welcome {
        ServerMessage::EndpointControl {
            kind: ENDPOINT_WELCOME_KIND.into(),
            data: serde_json::to_string(&welcome).map_err(io::Error::other)?,
        }
    } else {
        ServerMessage::Welcome {
            version: PROTOCOL_VERSION,
            encoding: render_encoding,
            error: None,
        }
    };
    protocol::write_message(&mut stream, &welcome).map_err(|e| io::Error::other(e.to_string()))?;

    set_client_recv_timeout(
        &stream,
        None,
        "failed to clear client handshake read timeout",
        client_id,
    )?;
    #[cfg(unix)]
    configure_client_write_buffer(&stream)?;

    // Create separate channels for reliable control messages and droppable renders.
    let writer_queue = ClientWriterQueue::new_with_snapshot_delta(snapshot_delta);
    writer_queue.set_disconnect_stream(stream.try_clone()?);
    let writer = ClientWriter {
        control: ClientControlWriter::queue(writer_queue.clone()),
        render: ClientRenderWriter::queue(writer_queue.clone()),
    };

    // Spawn a writer thread that forwards messages from the channels to the stream.
    let write_stream = stream.try_clone()?;
    let writer_event_tx = server_event_tx.clone();
    crate::thread_spawn::spawn_named("herdr-client-writer", move || {
        client_writer_loop(write_stream, client_id, writer_queue, writer_event_tx);
    })?;

    if should_quit.load(Ordering::Acquire) {
        send_shutdown_to_unregistered_client(&writer);
        return Ok(());
    }

    // Notify the main loop about the new client.
    let endpoint_control_writer = shell_options.as_ref().map(|_| writer.control.clone());
    let connected = if let Some((
        pixel_mouse,
        direct_graphics,
        endpoint_keybindings,
        mouse_capture,
        surface_active,
        surface_reuse,
        surface_delta,
        surface_scroll,
    )) = shell_options
    {
        ServerEvent::ClientShellConnected {
            client_id,
            surface_cols: client_cols,
            surface_rows: client_rows,
            cell_width_px,
            cell_height_px,
            pixel_mouse,
            direct_graphics,
            endpoint_keybindings,
            mouse_capture,
            surface_active,
            surface_reuse,
            surface_delta,
            surface_scroll,
            writer,
        }
    } else {
        ServerEvent::ClientConnected {
            client_id,
            cols: client_cols,
            rows: client_rows,
            cell_width_px,
            cell_height_px,
            pixel_mouse: terminal_pixel_mouse,
            writer,
        }
    };
    if let Err(err) = server_event_tx.blocking_send(connected) {
        match err.0 {
            ServerEvent::ClientConnected { writer, .. }
            | ServerEvent::ClientShellConnected { writer, .. } => {
                send_shutdown_to_unregistered_client(&writer);
            }
            _ => {}
        }
    }

    // Enter read loop — read client messages and forward to main loop.
    client_read_loop_with_endpoint_controls(
        stream,
        client_id,
        server_event_tx,
        should_quit,
        endpoint_control_writer.as_ref(),
    )
}

fn send_shutdown_to_unregistered_client(writer: &ClientWriter) {
    let mut framed = Vec::new();
    if protocol::write_message(
        &mut framed,
        &ServerMessage::ServerShutdown {
            reason: Some("server is shutting down".to_owned()),
        },
    )
    .is_ok()
    {
        let _ = writer.control.send(framed);
    }
}

/// The client writer loop — bounded control bursts, atomic presentations.
fn client_writer_loop(
    mut stream: LocalStream,
    client_id: u64,
    writer_queue: Arc<ClientWriterQueue>,
    server_event_tx: mpsc::Sender<ServerEvent>,
) {
    while let Some(item) = writer_queue.recv() {
        let written = match item {
            ClientWriteItem::Control(data) => write_framed_bytes(&mut stream, &data),
            ClientWriteItem::Render(data) => {
                let _ =
                    server_event_tx.blocking_send(ServerEvent::ClientWriterDrained { client_id });
                write_framed_bytes(&mut stream, &data)
            }
            ClientWriteItem::Presentation(projection, surface, snapshot_frame) => {
                let _ =
                    server_event_tx.blocking_send(ServerEvent::ClientWriterDrained { client_id });
                projection.iter().filter(|data| !data.is_empty())
                    .all(|data| write_framed_bytes(&mut stream, data))
                    && (snapshot_frame.is_empty() || write_framed_bytes(&mut stream, &snapshot_frame))
                    && (surface.is_empty() || write_framed_bytes(&mut stream, &surface))
            }
        };
        if !written {
            break;
        }
    }
    writer_queue.close_writer();
    let _ = server_event_tx.blocking_send(ServerEvent::ClientDisconnected { client_id });
    debug!("client writer thread exiting");
}

fn write_framed_bytes(stream: &mut LocalStream, data: &[u8]) -> bool {
    #[cfg(unix)]
    let result = crate::platform::write_client_stream(stream, data);
    #[cfg(windows)]
    let result = stream.write_all(data);
    if let Err(err) = result {
        debug!(err = %err, "client write failed, closing writer");
        return false;
    }
    if let Err(err) = stream.flush() {
        debug!(err = %err, "client flush failed, closing writer");
        return false;
    }
    true
}

/// The client read loop — reads messages from the client and forwards to the server event channel.
#[cfg(test)]
fn client_read_loop(
    stream: LocalStream,
    client_id: u64,
    server_event_tx: &mpsc::Sender<ServerEvent>,
    should_quit: &Arc<AtomicBool>,
) -> io::Result<()> {
    client_read_loop_with_endpoint_controls(stream, client_id, server_event_tx, should_quit, None)
}

fn client_read_loop_with_endpoint_controls(
    mut stream: LocalStream,
    client_id: u64,
    server_event_tx: &mpsc::Sender<ServerEvent>,
    should_quit: &Arc<AtomicBool>,
    endpoint_control_writer: Option<&ClientControlWriter>,
) -> io::Result<()> {
    while !should_quit.load(Ordering::Acquire) {
        #[cfg(unix)]
        let message = protocol::read_message(
            &mut crate::platform::ClientStreamReader(&mut stream),
            MAX_GRAPHICS_FRAME_SIZE,
        );
        #[cfg(windows)]
        let message = protocol::read_message(&mut stream, MAX_GRAPHICS_FRAME_SIZE);
        let msg: ClientMessage = match message {
            Ok(msg) => msg,
            Err(protocol::FramingError::UnexpectedEof) => {
                // Client disconnected.
                let _ =
                    server_event_tx.blocking_send(ServerEvent::ClientDisconnected { client_id });
                break;
            }
            Err(protocol::FramingError::Oversized { claimed, max }) => {
                warn!(
                    client_id,
                    claimed, max, "oversized message from client, closing"
                );
                let _ =
                    server_event_tx.blocking_send(ServerEvent::ClientDisconnected { client_id });
                break;
            }
            Err(err) => {
                debug!(client_id, err = %err, "client read error, closing");
                let _ =
                    server_event_tx.blocking_send(ServerEvent::ClientDisconnected { client_id });
                break;
            }
        };

        let event = match msg {
            ClientMessage::Input { data } => {
                // Validate input size.
                if data.len() > MAX_INPUT_PAYLOAD {
                    if crate::raw_input::is_complete_text_bracketed_paste(&data) {
                        warn!(
                            client_id,
                            size = data.len(),
                            max = MAX_INPUT_PAYLOAD,
                            "oversized bracketed paste from client, rejecting"
                        );
                        ServerEvent::ClientPasteRejected {
                            client_id,
                            size: data.len(),
                            max: MAX_INPUT_PAYLOAD,
                        }
                    } else {
                        warn!(
                            client_id,
                            size = data.len(),
                            "oversized input from client, closing"
                        );
                        let _ = server_event_tx
                            .blocking_send(ServerEvent::ClientDisconnected { client_id });
                        break;
                    }
                } else {
                    ServerEvent::ClientInput { client_id, data }
                }
            }
            ClientMessage::ObserveTerminal { target } => {
                #[cfg(unix)]
                {
                    // macOS Unix sockets can block even with per-send MSG_DONTWAIT.
                    // ClientStreamReader preserves blocking reads on the shared socket.
                    let configured = stream
                        .set_send_timeout(Some(OBSERVER_WRITE_TIMEOUT))
                        .and_then(|()| stream.set_nonblocking(true));
                    if let Err(err) = configured {
                        let _ = crate::platform::shutdown_client_stream(&stream);
                        let _ = server_event_tx
                            .blocking_send(ServerEvent::ClientDisconnected { client_id });
                        return Err(err);
                    }
                }
                ServerEvent::ClientObserveTerminal { client_id, target }
            }
            ClientMessage::ControlTerminal { target, takeover } => {
                ServerEvent::ClientControlTerminal {
                    client_id,
                    target,
                    takeover,
                }
            }
            ClientMessage::GraphicsTransmissionResult {
                transfer_id,
                image_id,
                success,
            } => ServerEvent::GraphicsTransmissionResult {
                client_id,
                transfer_id,
                image_id,
                success,
            },
            ClientMessage::GraphicsTransmissionStarted {
                transfer_id,
                image_id,
            } => ServerEvent::GraphicsTransmissionStarted {
                client_id,
                transfer_id,
                image_id,
            },
            ClientMessage::ClipboardImage {
                target,
                extension,
                data,
            } => {
                if data.len() > MAX_CLIPBOARD_IMAGE_PAYLOAD {
                    warn!(
                        client_id,
                        size = data.len(),
                        "oversized clipboard image from client, closing"
                    );
                    let _ = server_event_tx
                        .blocking_send(ServerEvent::ClientDisconnected { client_id });
                    break;
                } else {
                    ServerEvent::ClientClipboardImage {
                        client_id,
                        target,
                        extension,
                        data,
                    }
                }
            }
            ClientMessage::Resize {
                cols,
                rows,
                cell_width_px,
                cell_height_px,
                pixel_mouse,
            } => {
                let (clamped_cols, clamped_rows) = clamp_terminal_size(cols, rows);
                ServerEvent::ClientResize {
                    client_id,
                    cols: clamped_cols,
                    rows: clamped_rows,
                    cell_width_px,
                    cell_height_px,
                    pixel_mouse,
                }
            }
            ClientMessage::ClientShellResize {
                cell_width_px,
                cell_height_px,
                surface_size,
                pixel_mouse,
            } => {
                if let Some(reason) =
                    client_shell_geometry_error(surface_size, cell_width_px, cell_height_px)
                {
                    warn!(client_id, %reason, "invalid client shell resize, closing");
                    let _ = server_event_tx
                        .blocking_send(ServerEvent::ClientDisconnected { client_id });
                    break;
                }
                ServerEvent::ClientShellResize {
                    client_id,
                    surface_cols: surface_size.cols,
                    surface_rows: surface_size.rows,
                    cell_width_px,
                    cell_height_px,
                    pixel_mouse,
                }
            }
            ClientMessage::ClientShellHostTheme { update } => {
                if matches!(
                    &update,
                    crate::protocol::ClientHostThemeUpdate::PaletteColors(colors)
                        if colors.len() > 256
                ) {
                    warn!(client_id, "invalid client shell host theme update, closing");
                    let _ = server_event_tx
                        .blocking_send(ServerEvent::ClientDisconnected { client_id });
                    break;
                }
                ServerEvent::ClientShellHostTheme { client_id, update }
            }
            ClientMessage::ClientShellFocus { focused } => {
                ServerEvent::ClientShellFocus { client_id, focused }
            }
            ClientMessage::ClientShellMouseCapture { enabled } => {
                ServerEvent::ClientShellMouseCapture { client_id, enabled }
            }
            ClientMessage::ClientShellPaneInput { pane_id, events } => {
                match pane_input_event_limit(&events) {
                    InputEventLimit::WithinLimits => ServerEvent::ClientShellPaneInput {
                        client_id,
                        pane_id,
                        events,
                    },
                    InputEventLimit::TooManyEvents => {
                        warn!(
                            client_id,
                            count = events.len(),
                            "oversized targeted pane input batch, closing"
                        );
                        let _ = server_event_tx
                            .blocking_send(ServerEvent::ClientDisconnected { client_id });
                        break;
                    }
                    InputEventLimit::PasteTooLarge { size } => {
                        warn!(
                            client_id,
                            size,
                            max = MAX_INPUT_PAYLOAD,
                            "oversized targeted pane paste, rejecting"
                        );
                        ServerEvent::ClientPasteRejected {
                            client_id,
                            size,
                            max: MAX_INPUT_PAYLOAD,
                        }
                    }
                    InputEventLimit::InputPayloadTooLarge { size } => {
                        warn!(
                            client_id,
                            size,
                            max = MAX_INPUT_PAYLOAD,
                            "oversized targeted pane input, closing"
                        );
                        let _ = server_event_tx
                            .blocking_send(ServerEvent::ClientDisconnected { client_id });
                        break;
                    }
                }
            }
            ClientMessage::ClientShellPopupInput {
                terminal_id,
                events,
            } => match pane_input_event_limit(&events) {
                InputEventLimit::WithinLimits => ServerEvent::ClientShellPopupInput {
                    client_id,
                    terminal_id,
                    events,
                },
                InputEventLimit::TooManyEvents => {
                    warn!(
                        client_id,
                        count = events.len(),
                        "oversized popup input batch, closing"
                    );
                    let _ = server_event_tx
                        .blocking_send(ServerEvent::ClientDisconnected { client_id });
                    break;
                }
                InputEventLimit::PasteTooLarge { size } => {
                    warn!(
                        client_id,
                        size,
                        max = MAX_INPUT_PAYLOAD,
                        "oversized popup paste, rejecting"
                    );
                    ServerEvent::ClientPasteRejected {
                        client_id,
                        size,
                        max: MAX_INPUT_PAYLOAD,
                    }
                }
                InputEventLimit::InputPayloadTooLarge { size } => {
                    warn!(
                        client_id,
                        size,
                        max = MAX_INPUT_PAYLOAD,
                        "oversized popup input, closing"
                    );
                    let _ = server_event_tx
                        .blocking_send(ServerEvent::ClientDisconnected { client_id });
                    break;
                }
            },
            ClientMessage::ClientShellEndpointRequest { boot_id, request } => {
                if boot_id.len() > crate::server::client_commands::MAX_ENDPOINT_BOOT_ID_BYTES
                    || request.len() > crate::server::client_commands::MAX_ENDPOINT_COMMAND_BYTES
                {
                    warn!(
                        client_id,
                        boot_id_size = boot_id.len(),
                        request_size = request.len(),
                        "oversized client shell endpoint command, closing"
                    );
                    let _ = server_event_tx
                        .blocking_send(ServerEvent::ClientDisconnected { client_id });
                    break;
                }
                let decoded = match decode_endpoint_request(&request) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        warn!(client_id, %error, "invalid endpoint request envelope, closing");
                        let _ = server_event_tx
                            .blocking_send(ServerEvent::ClientDisconnected { client_id });
                        break;
                    }
                };
                let request_id = match &decoded {
                    DecodedEndpointRequest::Dispatch(request) => request.id.as_str(),
                    DecodedEndpointRequest::Error { request_id, .. } => request_id,
                };
                if request_id.len() > crate::server::client_commands::MAX_ENDPOINT_REQUEST_ID_BYTES
                {
                    warn!(
                        client_id,
                        "oversized client shell endpoint request id, closing"
                    );
                    let _ = server_event_tx
                        .blocking_send(ServerEvent::ClientDisconnected { client_id });
                    break;
                }
                match decoded {
                    DecodedEndpointRequest::Dispatch(request) => {
                        ServerEvent::ClientShellEndpointRequest {
                            client_id,
                            boot_id,
                            request,
                        }
                    }
                    DecodedEndpointRequest::Error {
                        request_id,
                        code,
                        message,
                    } => ServerEvent::ClientShellEndpointRequestError {
                        client_id,
                        boot_id,
                        request_id,
                        code,
                        message,
                    },
                }
            }
            ClientMessage::EndpointControl { kind, data }
                if kind == crate::protocol::endpoint::PRESENTATION_EFFECTS_SYNC_KIND =>
            {
                ServerEvent::ClientShellPresentationSync {
                    client_id,
                    token: data,
                }
            }
            ClientMessage::EndpointControl { kind, data } => {
                let Some(response) = crate::server::client_endpoint_control::response(&kind, data)
                else {
                    debug!(client_id, %kind, "ignoring unknown endpoint control message");
                    continue;
                };
                let Some(writer) = endpoint_control_writer else {
                    continue;
                };
                let mut framed = Vec::new();
                if protocol::write_message(&mut framed, &response).is_err()
                    || writer.send(framed).is_err()
                {
                    writer.queue.close_writer();
                    break;
                }
                continue;
            }
            ClientMessage::Detach => {
                let _ = server_event_tx.blocking_send(ServerEvent::ClientDetach { client_id });
                break;
            }
            ClientMessage::AttachTerminal {
                terminal_id,
                takeover,
            } => ServerEvent::ClientAttachTerminal {
                client_id,
                terminal_id,
                takeover,
            },
            ClientMessage::AttachScroll {
                source,
                direction,
                lines,
                column,
                row,
                modifiers,
            } => ServerEvent::ClientAttachScroll {
                client_id,
                source,
                direction,
                lines,
                column,
                row,
                modifiers,
            },
            ClientMessage::AttachMouse {
                kind,
                position,
                geometry,
                modifiers,
                lines,
            } => ServerEvent::ClientAttachMouse {
                client_id,
                kind,
                position,
                geometry,
                modifiers,
                lines,
            },
            ClientMessage::TerminalHello { .. } | ClientMessage::ClientShellHello { .. } => {
                // Duplicate handshake — ignore.
                continue;
            }
        };

        if server_event_tx.blocking_send(event).is_err() {
            break; // Main loop gone.
        }
    }

    debug!(client_id, "client read thread exiting");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use interprocess::local_socket::traits::Listener as _;
    use std::path::PathBuf;

    struct TestSocketPath(PathBuf);

    impl Drop for TestSocketPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn unique_test_path(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let filename = format!("h{}-{nanos}.sock", std::process::id());
        #[cfg(unix)]
        {
            let _ = name;
            PathBuf::from("/tmp").join(filename)
        }
        #[cfg(windows)]
        {
            std::env::temp_dir().join(format!("herdr-{name}-{filename}"))
        }
    }

    fn local_stream_pair(name: &str) -> (LocalStream, LocalStream, TestSocketPath) {
        let path = unique_test_path(name);
        let _ = std::fs::remove_file(&path);
        let listener = crate::ipc::bind_local_listener(&path).unwrap();
        let client = crate::ipc::connect_local_stream(&path).unwrap();
        let server = listener.accept().unwrap();
        (client, server, TestSocketPath(path))
    }

    fn endpoint_hello(surface_cols: u16, surface_rows: u16) -> ClientMessage {
        let hello = EndpointClientHello {
            generation: ENDPOINT_PROTOCOL_GENERATION,
            cell_width_px: 8,
            cell_height_px: 16,
            surface_size: crate::protocol::ClientSurfaceSize {
                cols: surface_cols,
                rows: surface_rows,
            },
            pixel_mouse: true,
            direct_graphics: true,
            endpoint_keybindings: true,
            mouse_capture: true,
            surface_active: true,
            surface_reuse: false,
            surface_delta: false,
            surface_scroll: false,
            snapshot_codecs: vec![crate::protocol::endpoint::SNAPSHOT_CODEC_V1.into()],
            surface_codecs: vec![crate::protocol::endpoint::SURFACE_CODEC_V1.into()],
            input_codecs: vec![crate::protocol::endpoint::INPUT_CODEC_V1.into()],
            blob_codecs: vec![crate::protocol::endpoint::BLOB_CODEC_V1.into()],
        };
        ClientMessage::EndpointControl {
            kind: ENDPOINT_HELLO_KIND.into(),
            data: serde_json::to_string(&hello).unwrap(),
        }
    }

    fn endpoint_welcome(message: ServerMessage) -> EndpointServerWelcome {
        let ServerMessage::EndpointControl { kind, data } = message else {
            panic!("expected endpoint welcome");
        };
        assert_eq!(kind, ENDPOINT_WELCOME_KIND);
        serde_json::from_str(&data).unwrap()
    }

    fn recv_server_event(receiver: &mut mpsc::Receiver<ServerEvent>, context: &str) -> ServerEvent {
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            match receiver.try_recv() {
                Ok(event) => return event,
                Err(mpsc::error::TryRecvError::Empty) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(err) => panic!("{context}: {err}"),
            }
        }
    }

    fn bracketed_paste_with_total_len(total_len: usize) -> Vec<u8> {
        const DELIMITER_BYTES: usize = b"\x1b[200~".len() + b"\x1b[201~".len();
        assert!(total_len >= DELIMITER_BYTES);
        let mut data = Vec::with_capacity(total_len);
        data.extend_from_slice(b"\x1b[200~");
        data.resize(total_len - b"\x1b[201~".len(), b'x');
        data.extend_from_slice(b"\x1b[201~");
        data
    }

    fn test_queue_writer() -> (ClientWriter, Arc<ClientWriterQueue>) {
        let queue = ClientWriterQueue::new();
        (
            ClientWriter {
                control: ClientControlWriter::queue(queue.clone()),
                render: ClientRenderWriter::queue(queue.clone()),
            },
            queue,
        )
    }

    fn frame_server_message(message: &ServerMessage) -> Vec<u8> {
        let mut bytes = Vec::new();
        protocol::write_message(&mut bytes, message).expect("frame server message");
        bytes
    }

    fn projection_frames(revision: u64, clear: bool) -> [Vec<u8>; 3] {
        let mut snapshot: protocol::ClientShellSnapshot = serde_json::from_str(include_str!(
            "../../tests/fixtures/endpoint-snapshot-v1.json"
        )).unwrap();
        snapshot.revision = revision;
        snapshot.agent_view_label = (!clear).then(|| "current".into());
        let view = crate::api::schema::AgentViewSetParams {
            source: "queue-test".into(), label: Some("current".into()),
            filter: None, sort: Vec::new(),
        };
        let completions = protocol::endpoint::EndpointAgentCompletions {
            boot_id: snapshot.boot_id.clone(), revision,
            completions: [("first".into(), 42), ("second".into(), 99)].into(),
        };
        [
            frame_server_message(&protocol::endpoint::agent_view_projection_message(
                &snapshot.boot_id, revision, (!clear).then_some(&view),
            ).unwrap()),
            frame_server_message(&protocol::endpoint::agent_completions_message(&completions).unwrap()),
            frame_server_message(&protocol::endpoint::snapshot_message(&snapshot, None, false).unwrap()),
        ]
    }
    #[test]
    fn typed_snapshot_backlog_deltas_reference_only_claimed_inventory() {
        for allow_delta in [false, true] {
            let queue = ClientWriterQueue::new_with_snapshot_delta(allow_delta);
            let writer = ClientWriter {
                control: ClientControlWriter::queue(queue.clone()),
                render: ClientRenderWriter::queue(queue.clone()),
            };
            let mut snapshot: protocol::ClientShellSnapshot = serde_json::from_str(include_str!(
                "../../tests/fixtures/endpoint-snapshot-v1.json"
            )).unwrap();
            snapshot.revision = 1;
            let original = snapshot.clone();
            writer.set_shell_projection([vec![1], vec![2]], Arc::new(snapshot.clone())).unwrap();
            writer.send_projection().unwrap();
            let Some(ClientWriteItem::Presentation(_, _, initial_snapshot)) = queue.recv() else { panic!() };
            let ServerMessage::EndpointControl { kind, .. } =
                protocol::read_message::<_, ServerMessage>(&mut initial_snapshot.as_slice(), MAX_FRAME_SIZE).unwrap()
            else { panic!() };
            assert_eq!(kind, protocol::endpoint::ENDPOINT_SNAPSHOT_KIND);
            let mut previous = std::sync::Weak::new();
            for revision in 2..=10_001 {
                snapshot.revision = revision;
                snapshot.agent_view_label = (revision != 10_001).then(|| format!("view {revision}"));
                writer.set_shell_projection([vec![1], vec![2]], Arc::new(snapshot.clone())).unwrap();
                writer.send_projection().unwrap();
                assert!(previous.upgrade().is_none(), "replaced inventory must be released");
                let state = queue.lock_state();
                assert!(state.control.is_empty() && state.ordered.is_empty());
                assert_eq!(state.claimed_snapshot.as_ref().unwrap().revision, 1);
                let Some(ClientWriteItem::Presentation(pending, _, snapshot_frame)) = &state.render else { panic!() };
                assert!(snapshot_frame.is_empty());
                assert!(pending[2].is_empty(), "do not encode replaceable snapshots");
                assert_eq!(Arc::strong_count(pending), 2);
                previous = Arc::downgrade(pending);
            }
            let Some(ClientWriteItem::Presentation(_, _, latest_snapshot)) = queue.recv() else { panic!() };
            let ServerMessage::EndpointControl { kind, data } =
                protocol::read_message::<_, ServerMessage>(&mut latest_snapshot.as_slice(), MAX_FRAME_SIZE).unwrap()
            else { panic!() };
            if allow_delta {
                assert_eq!(kind, protocol::endpoint::SNAPSHOT_DELTA_CODEC_V1);
                let delta: protocol::snapshot_delta::SnapshotDelta = serde_json::from_str(&data).unwrap();
                assert_eq!(delta.base_revision, 1);
                assert_eq!(protocol::snapshot_delta::apply_snapshot_delta(&original, delta).unwrap(), snapshot);
                assert!(latest_snapshot.len() * 4 < initial_snapshot.len());
            } else {
                assert_eq!(kind, protocol::endpoint::ENDPOINT_SNAPSHOT_KIND);
                assert_eq!(serde_json::from_str::<protocol::ClientShellSnapshot>(&data).unwrap(), snapshot);
            }
            writer.test_close();
            assert!(queue.lock_state().claimed_snapshot.is_none(), "disconnect drops every base");
        }
    }

    #[test]
    fn claimed_projection_encoding_releases_producers_and_preserves_racing_surface_commit() {
        let queue = ClientWriterQueue::new_with_snapshot_delta(true);
        let writer = ClientWriter {
            control: ClientControlWriter::queue(queue.clone()),
            render: ClientRenderWriter::queue(queue.clone()),
        };
        let mut snapshot: protocol::ClientShellSnapshot = serde_json::from_str(include_str!(
            "../../tests/fixtures/endpoint-snapshot-v1.json"
        )).unwrap();
        snapshot.revision = 1;
        snapshot.agent_view_label = Some("current".into());
        snapshot.server_keybindings_toml = Some("# large claimed configuration\n".repeat(4_000));
        let [view, completions, _] = projection_frames(1, false);
        let companion_addresses = (view.as_ptr() as usize, completions.as_ptr() as usize);
        writer.set_shell_projection([view, completions], Arc::new(snapshot.clone())).unwrap();
        let ServerMessage::PaneSurface(surface) =
            protocol::read_message::<_, ServerMessage>(&mut test_surface_frame(1, "A").as_slice(), MAX_FRAME_SIZE).unwrap()
        else { panic!("initial surface") };
        let mut render_state = crate::server::render_stream::ClientRenderState::new(RenderEncoding::SemanticFrame);
        render_state.enable_surface_delta(true);
        let prepared = render_state.prepare_pane_surface(surface).unwrap();
        let initial_message = prepared.message().clone();
        let initial_token = writer.render.try_send_tracked(frame_server_message(&initial_message)).unwrap();

        let (entered, entered_rx) = std::sync::mpsc::channel();
        let resume = Arc::new(std::sync::Barrier::new(2));
        queue.projection_encode_gate.set((entered, resume.clone(), AtomicBool::new(false))).unwrap();
        let drain = queue.clone();
        let consumer = std::thread::spawn(move || drain.recv().unwrap());
        entered_rx.recv_timeout(Duration::from_secs(2)).expect("writer reaches unlocked encoding");
        assert!(initial_token.load(Ordering::Acquire));
        // Force claim between tracked enqueue and the server's baseline commit.
        render_state.commit_replaceable_frame(prepared, initial_token);
        let claimed_surface = render_state.last_pane_surface().unwrap().clone();
        let expected_claimed = claimed_surface.clone();
        let (finished, finished_rx) = std::sync::mpsc::channel();
        let producer_writer = writer.clone();
        let producer = std::thread::spawn(move || {
            let clipboard = frame_server_message(&ServerMessage::Clipboard { data: "reliable".into() });
            producer_writer.control.send(clipboard.clone()).unwrap();
            let mut expected_snapshot = snapshot;
            for revision in [2, 3] {
                expected_snapshot.revision = revision;
                expected_snapshot.config_diagnostic = Some(format!("revision {revision}"));
                let [view, completions, _] = projection_frames(revision, false);
                producer_writer.set_shell_projection(
                    [view, completions], Arc::new(expected_snapshot.clone()),
                ).unwrap();
                let mut next = claimed_surface.clone();
                next.projection_revision = revision;
                next.frame.cells[0].symbol = format!("visible-{revision}");
                let prepared = render_state.prepare_pane_surface(next).unwrap();
                let token = producer_writer.render.try_send_tracked(
                    frame_server_message(prepared.message()),
                ).unwrap();
                render_state.commit_replaceable_frame(prepared, token);
                if revision == 2 {
                    assert!(producer_writer.discard_pending_render());
                    render_state.discard_pending_frame();
                    assert_eq!(render_state.last_pane_surface(), Some(&claimed_surface),
                        "replacement must keep a frame claimed before its server commit");
                }
            }
            let expected_surface = render_state.last_pane_surface().unwrap().clone();
            finished.send(()).unwrap();
            (expected_snapshot, expected_surface, clipboard)
        });
        // This is a liveness deadline, not a serialization performance threshold.
        // Always release the encoder before asserting, including a locking regression.
        let producer_finished_while_encoding = finished_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        resume.wait();
        let first = consumer.join().unwrap();
        let (expected_snapshot, expected_surface, clipboard) = producer.join().unwrap();
        assert!(producer_finished_while_encoding, "encoding must not hold the producer queue mutex");
        let ClientWriteItem::Presentation(first_projection, first_surface, first_snapshot) = first else { panic!() };
        assert_eq!(
            (first_projection[0].as_ptr() as usize, first_projection[1].as_ptr() as usize),
            companion_addresses,
            "claim must borrow the original immutable companion buffers",
        );
        let mut snapshot_decoder = protocol::snapshot_delta::SnapshotDecoder::new(true);
        let decode_snapshot = |decoder: &mut protocol::snapshot_delta::SnapshotDecoder, mut frame: &[u8]| {
            let ServerMessage::EndpointControl { kind, data } =
                protocol::read_message::<_, ServerMessage>(&mut frame, MAX_FRAME_SIZE).unwrap()
            else { panic!() };
            decoder.decode(&kind, &data).unwrap().unwrap()
        };
        let _ = decode_snapshot(&mut snapshot_decoder, &first_snapshot);
        let mut surface_decoder = protocol::surface_reuse::Decoder::new(true, false);
        let first_surface: ServerMessage = protocol::read_message(&mut first_surface.as_slice(), MAX_FRAME_SIZE).unwrap();
        assert_eq!(surface_decoder.decode(first_surface).unwrap(), ServerMessage::PaneSurface(expected_claimed));
        assert_eq!(queue.recv(), Some(ClientWriteItem::Control(clipboard)));
        let Some(ClientWriteItem::Presentation(_, latest_surface, latest_snapshot)) = queue.recv() else { panic!() };
        assert_eq!(*decode_snapshot(&mut snapshot_decoder, &latest_snapshot), expected_snapshot);
        let latest_surface: ServerMessage = protocol::read_message(&mut latest_surface.as_slice(), MAX_FRAME_SIZE).unwrap();
        assert_eq!(surface_decoder.decode(latest_surface).unwrap(), ServerMessage::PaneSurface(expected_surface));
    }

    #[test]
    fn reliable_bootstrap_prefix_cannot_be_overtaken_by_delta_fairness() {
        let (writer, queue) = test_queue_writer();
        let snapshot: protocol::ClientShellSnapshot = serde_json::from_str(include_str!(
            "../../tests/fixtures/endpoint-snapshot-v1.json"
        )).unwrap();
        for byte in 0..(CONTROL_BURST as u8 + 4) {
            writer.control.send(vec![byte]).unwrap();
        }
        queue.lock_state().control_burst = CONTROL_BURST;
        writer.seed_ordered_snapshot(Arc::new(snapshot));
        writer.render.try_send(vec![255]).unwrap();
        for byte in 0..(CONTROL_BURST as u8 + 4) {
            assert_eq!(queue.recv(), Some(ClientWriteItem::Control(vec![byte])));
        }
        assert_eq!(queue.recv(), Some(ClientWriteItem::Render(vec![255])));
    }

    #[test]
    fn render_claim_tokens_distinguish_discard_claim_and_ordered_promotion() {
        let (writer, queue) = test_queue_writer();
        let discarded = writer.render.try_send_tracked(vec![1]).unwrap();
        assert!(writer.discard_pending_render());
        assert!(!discarded.load(Ordering::Acquire));
        let claimed = writer.render.try_send_tracked(vec![2]).unwrap();
        assert_eq!(queue.recv(), Some(ClientWriteItem::Render(vec![2])));
        assert!(claimed.load(Ordering::Acquire));
        let ordered = writer.render.try_send_tracked(vec![3]).unwrap();
        writer.render.send_ordered(vec![4]).unwrap();
        assert!(ordered.load(Ordering::Acquire), "ordered work cannot be discarded");
        assert_eq!(queue.recv(), Some(ClientWriteItem::Render(vec![3])));
        assert_eq!(queue.recv(), Some(ClientWriteItem::Render(vec![4])));
    }


    #[test]
    fn projection_backlog_retains_only_latest_complete_clear_and_completions() {
        let (writer, queue) = test_queue_writer();
        let mut previous = std::sync::Weak::new();
        for revision in 1..=10_000 {
            writer.set_projection(projection_frames(revision, revision == 10_000)).unwrap();
            writer.send_projection().unwrap();
            assert!(previous.upgrade().is_none(), "replaced projection payload must be released");
            let state = queue.lock_state();
            assert!(state.control.is_empty());
            assert_eq!(state.control_bytes, 0);
            assert!(state.ordered.is_empty());
            let Some(ClientWriteItem::Presentation(pending, surface, _)) = &state.render else {
                panic!("one complete pending presentation");
            };
            assert!(surface.is_empty());
            assert!(Arc::ptr_eq(pending, state.projection.as_ref().unwrap()));
            assert_eq!(Arc::strong_count(pending), 2, "no hidden projection backlog");
            assert!(pending.iter().map(Vec::len).sum::<usize>() < 64 * 1024);
            previous = Arc::downgrade(pending);
        }
        let Some(ClientWriteItem::Presentation(frames, _, _)) = queue.recv() else {
            panic!("latest complete projection");
        };
        let mut messages = frames.iter().map(|bytes| {
            protocol::read_message::<_, ServerMessage>(&mut bytes.as_slice(), MAX_FRAME_SIZE).unwrap()
        });
        let ServerMessage::EndpointControl { data, .. } = messages.next().unwrap() else { panic!() };
        let view: protocol::endpoint::EndpointAgentViewProjection = serde_json::from_str(&data).unwrap();
        assert_eq!(view.revision, 10_000);
        assert!(view.view.is_none());
        let ServerMessage::EndpointControl { data, .. } = messages.next().unwrap() else { panic!() };
        let completions: protocol::endpoint::EndpointAgentCompletions = serde_json::from_str(&data).unwrap();
        assert_eq!(completions.boot_id, view.boot_id);
        assert_eq!(completions.revision, view.revision);
        assert_eq!(completions.completions["first"], 42);
        assert_eq!(completions.completions["second"], 99);
        let ServerMessage::EndpointControl { data, .. } = messages.next().unwrap() else { panic!() };
        let snapshot: protocol::ClientShellSnapshot = serde_json::from_str(&data).unwrap();
        assert_eq!(snapshot.boot_id, view.boot_id);
        assert_eq!(snapshot.revision, view.revision);
        assert!(snapshot.agent_view_label.is_none());
    }

    #[test]
    fn reliable_fifo_is_exactly_once_and_cannot_starve_presentations() {
        let (writer, queue) = test_queue_writer();
        let mut expected = VecDeque::new();
        for number in 0..64 {
            let message = match number % 5 {
                0 => ServerMessage::Clipboard { data: number.to_string() },
                1 => ServerMessage::Notify {
                    kind: protocol::NotifyKind::Toast,
                    message: number.to_string(), body: None,
                },
                2 => ServerMessage::ClientShellEndpointResponseChunk {
                    boot_id: "boot".into(), request_id: "request".into(),
                    final_chunk: number == 62, data: vec![number as u8],
                },
                3 => ServerMessage::ClientShellKeyboardReportAll { enabled: number % 2 == 0 },
                _ => ServerMessage::MouseCapture { enabled: number % 2 == 0, sgr_pixels: false },
            };
            let frame = frame_server_message(&message);
            expected.push_back(frame.clone());
            writer.control.send(frame).unwrap();
        }
        writer.set_projection(projection_frames(1, false)).unwrap();
        writer.render.try_send(vec![1]).unwrap();
        let mut before_paint = 0;
        loop {
            let claimed = {
                let mut state = queue.lock_state();
                queue.pop(&mut state)
            };
            let Some(claimed) = claimed else { break };
            match queue.materialize(claimed).unwrap() {
                ClientWriteItem::Control(data) => {
                    assert_eq!(Some(data), expected.pop_front());
                    before_paint += 1;
                }
                ClientWriteItem::Presentation(_, surface, _) => {
                    assert_eq!(surface, [1]);
                    assert_eq!(before_paint, CONTROL_BURST);
                    break;
                }
                _ => panic!("complete presentation expected"),
            }
        }
        while let Some(data) = expected.pop_front() {
            assert_eq!(queue.recv(), Some(ClientWriteItem::Control(data)));
        }
        assert_eq!(queue.lock_state().control_bytes, 0);
    }

    #[test]
    fn reliable_byte_budget_accepts_legal_frames_then_disconnects_and_releases() {
        let (writer, queue) = test_queue_writer();
        let (healthy, healthy_queue) = test_queue_writer();
        let legal = MAX_GRAPHICS_FRAME_SIZE + 4;
        writer.control.send(vec![0; legal]).unwrap();
        writer.control.send(vec![0; legal]).unwrap();
        assert!(writer.control.send(vec![0]).is_err());
        let state = queue.lock_state();
        assert!(!state.writer_alive);
        assert!(state.control.is_empty());
        assert_eq!(state.control.capacity(), 0);
        assert_eq!(state.control_bytes, 0);
        assert!(state.render.is_none() && state.ordered.is_empty() && state.projection.is_none());
        drop(state);
        assert_eq!(queue.recv(), None);
        healthy.control.send(vec![7]).unwrap();
        assert_eq!(healthy_queue.recv(), Some(ClientWriteItem::Control(vec![7])));
    }

    #[cfg(unix)]
    #[test]
    fn reliable_overflow_wakes_inflight_writer_and_reader_without_affecting_peer() {
        let (_client, server, _path) = local_stream_pair("reliable-overflow");
        let (writer, queue) = test_queue_writer();
        queue.set_disconnect_stream(server.try_clone().unwrap());
        let write_stream = server.try_clone().unwrap();
        let (events, mut event_rx) = mpsc::channel(8);
        let read_events = events.clone();
        let (read_done_tx, read_done) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let _ = client_read_loop(server, 77, &read_events, &Arc::new(AtomicBool::new(false)));
            read_done_tx.send(()).unwrap();
        });
        writer.render.try_send(vec![0; 8 * 1024 * 1024]).unwrap();
        let (write_done_tx, write_done) = std::sync::mpsc::channel();
        let worker_queue = queue.clone();
        let worker = std::thread::spawn(move || {
            client_writer_loop(write_stream, 77, worker_queue, events);
            write_done_tx.send(()).unwrap();
        });
        assert!(matches!(event_rx.blocking_recv(), Some(ServerEvent::ClientWriterDrained { client_id: 77 })));
        for _ in 0..MAX_PENDING_CONTROL_ITEMS {
            writer.control.send(vec![1]).unwrap();
        }
        assert!(writer.control.send(vec![2]).is_err());
        read_done.recv_timeout(Duration::from_secs(2)).expect("overflow wakes blocked reader");
        write_done.recv_timeout(Duration::from_secs(2)).expect("overflow wakes blocked writer");
        reader.join().unwrap();
        worker.join().unwrap();
        assert_eq!(queue.lock_state().control_bytes, 0);
        assert!(matches!(writer.render.try_send(vec![3]), Err(TrySendError::Disconnected(_))));
        let (healthy, healthy_queue) = test_queue_writer();
        healthy.control.send(vec![4]).unwrap();
        assert_eq!(healthy_queue.recv(), Some(ClientWriteItem::Control(vec![4])));
    }

    fn test_surface_frame(revision: u64, symbol: &str) -> Vec<u8> {
        let buffer = ratatui::buffer::Buffer::with_lines(
            (0..80).map(|_| symbol.repeat(120)).collect::<Vec<_>>(),
        );
        frame_server_message(&ServerMessage::PaneSurface(protocol::PaneSurfaceFrame {
            boot_id: "boot-v1".into(),
            projection_revision: revision,
            surface_revision: revision,
            frame: protocol::FrameData::from_ratatui_buffer_with_hyperlinks(&buffer, None, &[]),
            panes: Vec::new(), splits: Vec::new(), popup: None, graphics: Default::default(),
        }))
    }

    #[test]
    fn graphics_batches_pin_their_projection_across_newer_metadata_and_resync() {
        let (writer, queue) = test_queue_writer();
        let inline = frame_server_message(&ServerMessage::Graphics {
            bytes: b"\x1b_Ga=T,f=32,s=1,v=1,i=7;/wAA/w==\x1b\\".to_vec(),
        });
        let mut direct = frame_server_message(&ServerMessage::GraphicsFile {
            path: "/private-test/image.rgba".into(), expected_len: 4,
            image_id: 7, transfer_id: 1, leading: Vec::new(),
            control: "a=T,f=32,s=1,v=1,i=7".into(), surface_asset: None,
        });
        direct.extend(frame_server_message(&ServerMessage::EndpointControl {
            kind: protocol::endpoint::PRESENTATION_EFFECTS_READY_KIND.into(), data: "fence".into(),
        }));
        let resync = test_surface_frame(2, "B");
        writer.set_projection(projection_frames(1, false)).unwrap();
        writer.render.try_send_protected(inline.clone()).unwrap();
        assert!(!writer.discard_pending_render(), "inline asset delivery cannot be rolled back");
        writer.render.send_ordered(direct.clone()).unwrap();
        for _ in 0..CONTROL_BURST {
            writer.control.send(frame_server_message(&ServerMessage::ReloadSoundConfig)).unwrap();
        }
        let retired = frame_server_message(&ServerMessage::GraphicsTransmissionRetired {
            transfer_id: 1, image_id: 7,
        });
        writer.control.send(retired.clone()).unwrap();
        for _ in 0..CONTROL_BURST {
            assert_eq!(queue.recv(), Some(ClientWriteItem::Control(frame_server_message(&ServerMessage::ReloadSoundConfig))));
        }
        assert_eq!(queue.recv(), Some(ClientWriteItem::Control(retired)), "retirement must precede queued file despite control burst limit");
        writer.set_projection(projection_frames(2, true)).unwrap();
        writer.render.try_send(test_surface_frame(2, "X")).unwrap();
        assert!(writer.discard_pending_render());
        writer.render.try_send(resync.clone()).unwrap();
        for (revision, expected) in [(1, inline), (1, direct), (2, resync)] {
            let item = queue.recv().unwrap();
            let surface = match item {
                ClientWriteItem::Presentation(frames, data, _) => {
                    let ServerMessage::EndpointControl { data: snapshot, .. } =
                        protocol::read_message::<_, ServerMessage>(&mut frames[2].as_slice(), MAX_FRAME_SIZE).unwrap()
                    else { panic!() };
                    let snapshot: protocol::ClientShellSnapshot = serde_json::from_str(&snapshot).unwrap();
                    assert_eq!(snapshot.revision, revision);
                    data
                }
                ClientWriteItem::Render(data) if revision == 1 => data,
                _ => panic!("coherent graphics ordering"),
            };
            assert_eq!(surface, expected);
        }
    }

    #[cfg(unix)]
    #[test]
    fn claimed_partial_presentation_finishes_before_newer_projection_and_surface() {
        use std::os::fd::AsRawFd as _;
        let (mut client, server, _path) = local_stream_pair("inflight-presentation");
        let LocalStream::UdSocket(socket) = &server;
        let size: libc::c_int = 4096;
        assert_eq!(unsafe {
            libc::setsockopt(socket.inner().as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF,
                (&size as *const libc::c_int).cast(), std::mem::size_of_val(&size) as libc::socklen_t)
        }, 0);
        client.set_recv_timeout(Some(Duration::from_secs(2))).unwrap();
        let (writer, queue) = test_queue_writer();
        queue.set_disconnect_stream(server.try_clone().unwrap());
        writer.set_projection(projection_frames(1, false)).unwrap();
        writer.render.try_send(test_surface_frame(1, "A")).unwrap();
        let (events, mut event_rx) = mpsc::channel(8);
        let worker = std::thread::spawn(move || client_writer_loop(server, 88, queue, events));
        assert!(matches!(event_rx.blocking_recv(), Some(ServerEvent::ClientWriterDrained { client_id: 88 })));
        assert!(!writer.discard_pending_render(), "a claimed/partial frame is never replaced");
        writer.set_projection(projection_frames(2, true)).unwrap();
        writer.render.try_send(test_surface_frame(2, "B")).unwrap();
        for revision in [1, 2] {
            for kind in [
                protocol::endpoint::AGENT_VIEW_PROJECTION_KIND,
                protocol::endpoint::AGENT_COMPLETIONS_KIND,
                protocol::endpoint::ENDPOINT_SNAPSHOT_KIND,
            ] {
                let message: ServerMessage = protocol::read_message(&mut client, MAX_FRAME_SIZE).unwrap();
                let ServerMessage::EndpointControl { kind: actual, data } = message else { panic!() };
                assert_eq!(actual, kind);
                let projection: serde_json::Value = serde_json::from_str(&data).unwrap();
                assert_eq!(projection["boot_id"], "boot-v1");
                assert_eq!(projection["revision"], revision);
            }
            let message: ServerMessage = protocol::read_message(&mut client, MAX_FRAME_SIZE).unwrap();
            let ServerMessage::PaneSurface(surface) = message else { panic!() };
            assert_eq!(surface.projection_revision, revision);
            assert_eq!(surface.boot_id, "boot-v1");
            let expected = if revision == 1 { "A" } else { "B" };
            assert!(surface.frame.cells.iter().all(|cell| cell.symbol == expected));
        }
        drop(writer);
        worker.join().unwrap();
    }

    #[test]
    fn client_writer_queue_keeps_render_slot_bounded() {
        let (writer, _queue) = test_queue_writer();
        let first = frame_server_message(&ServerMessage::WindowTitle {
            title: Some("first".into()),
        });
        let second = frame_server_message(&ServerMessage::WindowTitle {
            title: Some("second".into()),
        });

        writer.render.try_send(first).expect("first render fits");
        assert!(matches!(
            writer.render.try_send(second),
            Err(TrySendError::Full(_))
        ));
    }

    #[test]
    fn ordered_direct_follows_older_render_and_stays_bounded() {
        let (writer, queue) = test_queue_writer();
        writer.render.try_send(b"old".to_vec()).unwrap();
        writer.render.send_ordered(b"direct".to_vec()).unwrap();
        assert!(matches!(
            writer.render.send_ordered(b"second".to_vec()),
            Err(TrySendError::Full(_))
        ));
        writer.render.try_send(b"new".to_vec()).unwrap();

        for expected in [b"old".as_slice(), b"direct", b"new"] {
            assert_eq!(
                queue.recv(),
                Some(ClientWriteItem::Render(expected.to_vec()))
            );
        }
        queue.close_writer();
        assert!(matches!(
            writer.render.send_ordered(b"closed".to_vec()),
            Err(TrySendError::Disconnected(_))
        ));
    }

    #[test]
    fn client_writer_prioritizes_control_and_reports_render_drain() {
        let (mut client_stream, server_stream, _path) = local_stream_pair("client-writer-priority");
        let (writer, queue) = test_queue_writer();
        writer
            .render
            .try_send(frame_server_message(&ServerMessage::WindowTitle {
                title: Some("render".into()),
            }))
            .expect("queue render");
        writer
            .control
            .send(frame_server_message(&ServerMessage::ReloadSoundConfig))
            .expect("queue control");

        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let handle = std::thread::spawn(move || {
            client_writer_loop(server_stream, 9, queue, server_event_tx);
        });

        match protocol::read_message(&mut client_stream, MAX_FRAME_SIZE).expect("read control") {
            ServerMessage::ReloadSoundConfig => {}
            other => panic!("expected control message first, got {other:?}"),
        }
        match protocol::read_message(&mut client_stream, MAX_FRAME_SIZE).expect("read render") {
            ServerMessage::WindowTitle { title } => assert_eq!(title.as_deref(), Some("render")),
            other => panic!("expected render message second, got {other:?}"),
        }
        match server_event_rx
            .blocking_recv()
            .expect("writer drained render slot")
        {
            ServerEvent::ClientWriterDrained { client_id } => assert_eq!(client_id, 9),
            other => panic!("expected writer drained event, got {other:?}"),
        }

        drop(writer);
        handle.join().expect("writer exits after senders drop");
    }

    #[test]
    fn client_writer_exits_when_all_writer_handles_drop() {
        let (_client_stream, server_stream, _path) = local_stream_pair("client-writer-drop");
        let (writer, queue) = test_queue_writer();
        let (server_event_tx, _server_event_rx) = mpsc::channel(4);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            client_writer_loop(server_stream, 11, queue, server_event_tx);
            let _ = done_tx.send(());
        });

        drop(writer);
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("writer exits without polling after senders drop");
    }

    #[test]
    fn client_writer_clone_keeps_loop_alive_until_final_drop() {
        let (mut client_stream, server_stream, _path) =
            local_stream_pair("client-writer-clone-drop");
        let (writer, queue) = test_queue_writer();
        let cloned_writer = writer.clone();
        let (server_event_tx, _server_event_rx) = mpsc::channel(4);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            client_writer_loop(server_stream, 12, queue, server_event_tx);
            let _ = done_tx.send(());
        });

        drop(writer);
        cloned_writer
            .control
            .send(frame_server_message(&ServerMessage::ReloadSoundConfig))
            .expect("cloned writer still sends after original drops");
        match protocol::read_message(&mut client_stream, MAX_FRAME_SIZE)
            .expect("read control from cloned writer")
        {
            ServerMessage::ReloadSoundConfig => {}
            other => panic!("expected cloned control message, got {other:?}"),
        }
        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "writer exited while cloned handles were still alive"
        );

        drop(cloned_writer);
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("writer exits after final cloned writer drops");
    }

    #[test]
    fn client_writer_closes_queue_after_socket_write_failure() {
        let (client_stream, server_stream, _path) =
            local_stream_pair("client-writer-socket-failure");
        #[cfg(not(windows))]
        server_stream
            .set_send_timeout(Some(Duration::from_millis(100)))
            .expect("set test send timeout");
        let (writer, queue) = test_queue_writer();
        let (server_event_tx, _server_event_rx) = mpsc::channel(4);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            client_writer_loop(server_stream, 13, queue, server_event_tx);
            let _ = done_tx.send(());
        });

        drop(client_stream);
        writer
            .control
            .send(vec![b'x'; 1024 * 1024])
            .expect("message is accepted before the writer observes socket failure");
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer exits after socket write failure");

        assert!(matches!(writer.control.send(vec![b'y']), Err(SendError(_))));
        assert!(matches!(
            writer.render.try_send(vec![b'z']),
            Err(TrySendError::Disconnected(_))
        ));
    }

    /// Keeps both kernel socket buffers far below the frame size so the writer must
    /// block for the observer instead of accepting the whole frame at once. Without
    /// this the reset path would only be exercised when the platform default buffers
    /// happen to be smaller than the frame.
    #[cfg(unix)]
    fn limit_local_stream_buffers(stream: &LocalStream) {
        use std::os::fd::AsRawFd as _;

        let LocalStream::UdSocket(socket) = stream;
        let bytes: libc::c_int = 64 * 1024;
        for option in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
            let result = unsafe {
                libc::setsockopt(
                    socket.inner().as_raw_fd(),
                    libc::SOL_SOCKET,
                    option,
                    std::ptr::addr_of!(bytes).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            assert_eq!(result, 0, "setsockopt({option}) failed");
        }
    }

    #[cfg(unix)]
    #[test]
    fn observer_write_timeout_resets_when_sending_makes_progress() {
        use std::io::Read as _;

        // A slow observer that keeps draining must not be disconnected, because every
        // partial write resets the writer's deadline. Keep the drain cadence far below
        // the write timeout so a loaded runner's scheduler jitter cannot manufacture a
        // stall, while the transfer still outlasts the timeout so the reset is required
        // (otherwise the writer could finish within one timeout window without resetting).
        const WRITE_TIMEOUT: Duration = Duration::from_secs(1);
        const DRAIN_INTERVAL: Duration = Duration::from_millis(25);
        const TOTAL_BYTES: usize = 1024 * 1024;

        let (mut client, mut server, _path) = local_stream_pair("slow-observer");
        limit_local_stream_buffers(&server);
        limit_local_stream_buffers(&client);
        server.set_send_timeout(Some(WRITE_TIMEOUT)).unwrap();
        server.set_nonblocking(true).unwrap();
        let worker = std::thread::spawn(move || {
            assert!(write_framed_bytes(&mut server, &vec![b'x'; TOTAL_BYTES]));
        });
        client
            .set_recv_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut received = 0;
        let mut buffer = [0; 16 * 1024];
        while received < TOTAL_BYTES {
            let count = client.read(&mut buffer).unwrap();
            assert_ne!(count, 0, "observer disconnected while making progress");
            received += count;
            std::thread::sleep(DRAIN_INTERVAL);
        }
        worker.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn stalled_observer_timeout_releases_writer_and_reader() {
        let (mut client, server, _path) = local_stream_pair("stalled-observer");
        configure_client_write_buffer(&server).unwrap();
        let writer_stream = server.try_clone().expect("clone writer stream");
        let (writer, queue) = test_queue_writer();
        let (events, event_rx) = mpsc::channel(8);
        let reader_events = events.clone();
        let (reader_done_tx, reader_done) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let result = client_read_loop(
                server,
                14,
                &reader_events,
                &Arc::new(AtomicBool::new(false)),
            );
            let _ = reader_done_tx.send(result);
        });
        protocol::write_message(
            &mut client,
            &ClientMessage::ObserveTerminal {
                target: "w1:p1".into(),
            },
        )
        .expect("observe request");
        let mut event_rx = event_rx;
        assert!(matches!(
            event_rx.blocking_recv(),
            Some(ServerEvent::ClientObserveTerminal { client_id: 14, .. })
        ));
        let LocalStream::UdSocket(socket) = &writer_stream;
        assert_eq!(
            socket.inner().write_timeout().unwrap(),
            Some(Duration::from_secs(30))
        );
        assert_eq!(socket.inner().read_timeout().unwrap(), None);
        let mut resize = Vec::new();
        protocol::write_message(
            &mut resize,
            &ClientMessage::Resize {
                cols: 80,
                rows: 24,
                cell_width_px: 0,
                cell_height_px: 0,
                pixel_mouse: false,
            },
        )
        .unwrap();
        client.write_all(&resize[..2]).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        client.write_all(&resize[2..]).unwrap();
        assert!(matches!(
            event_rx.blocking_recv(),
            Some(ServerEvent::ClientResize { client_id: 14, .. })
        ));
        writer_stream
            .set_send_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let (writer_done_tx, writer_done) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            client_writer_loop(writer_stream, 14, queue, events);
            let _ = writer_done_tx.send(());
        });
        writer.render.try_send(vec![0; 4 * 1024 * 1024]).unwrap();
        writer_done
            .recv_timeout(Duration::from_millis(350))
            .expect("writer timed out");
        reader_done
            .recv_timeout(Duration::from_secs(2))
            .expect("reader released")
            .unwrap();
        worker.join().unwrap();
        reader.join().unwrap();
        assert!(writer.control.send(vec![1]).is_err());
    }

    #[test]
    fn clamp_terminal_size_zero_zero() {
        assert_eq!(
            clamp_terminal_size(0, 0),
            (MIN_CLIENT_COLS, MIN_CLIENT_ROWS)
        );
    }

    #[test]
    fn clamp_terminal_size_one_one() {
        assert_eq!(clamp_terminal_size(1, 1), (1, 1));
    }

    #[test]
    fn clamp_terminal_size_preserves_narrow_client_size() {
        assert_eq!(clamp_terminal_size(40, 12), (40, 12));
    }

    #[test]
    fn clamp_terminal_size_valid() {
        assert_eq!(clamp_terminal_size(120, 40), (120, 40));
    }

    #[test]
    fn clamp_terminal_size_exact_minimum() {
        assert_eq!(
            clamp_terminal_size(MIN_CLIENT_COLS, MIN_CLIENT_ROWS),
            (MIN_CLIENT_COLS, MIN_CLIENT_ROWS)
        );
    }

    #[test]
    fn client_shell_geometry_rejects_unsafe_dimensions_and_cell_sizes() {
        assert!(client_shell_geometry_error(
            crate::protocol::ClientSurfaceSize { cols: 80, rows: 24 },
            8,
            16,
        )
        .is_none());
        assert!(client_shell_geometry_error(
            crate::protocol::ClientSurfaceSize {
                cols: MAX_CLIENT_SHELL_DIMENSION,
                rows: MAX_CLIENT_SHELL_DIMENSION,
            },
            8,
            16,
        )
        .is_some());
        assert!(client_shell_geometry_error(
            crate::protocol::ClientSurfaceSize { cols: 80, rows: 24 },
            MAX_CLIENT_CELL_SIZE_PX + 1,
            16,
        )
        .is_some());
    }

    #[test]
    fn unknown_endpoint_method_returns_correlated_error() {
        let decoded = decode_endpoint_request(
            r#"{"id":"req-1","method":"plugin.future","params":{"value":1}}"#,
        )
        .unwrap();
        assert!(matches!(
            decoded,
            DecodedEndpointRequest::Error {
                request_id,
                code: "unsupported_method",
                ..
            } if request_id == "req-1"
        ));
    }

    #[test]
    fn malformed_known_endpoint_method_returns_correlated_error() {
        let decoded =
            decode_endpoint_request(r#"{"id":"req-2","method":"workspace.focus","params":{}}"#)
                .unwrap();
        assert!(matches!(
            decoded,
            DecodedEndpointRequest::Error {
                request_id,
                code: "invalid_request",
                ..
            } if request_id == "req-2"
        ));
    }

    #[test]
    fn handshake_negotiates_terminal_ansi_encoding() {
        let (mut client_stream, server_stream, _path) = local_stream_pair("client-handshake-ansi");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let handshake_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            handle_client_handshake(server_stream, 42, &server_event_tx, &handshake_quit)
        });

        protocol::write_message(
            &mut client_stream,
            &ClientMessage::TerminalHello {
                version: PROTOCOL_VERSION,
                cols: 100,
                rows: 30,
                cell_width_px: 8,
                cell_height_px: 16,
                pixel_mouse: true,
            },
        )
        .expect("write hello");

        let welcome: ServerMessage =
            protocol::read_message(&mut client_stream, MAX_FRAME_SIZE).expect("read welcome");
        match welcome {
            ServerMessage::Welcome {
                version,
                encoding,
                error,
            } => {
                assert_eq!(version, PROTOCOL_VERSION);
                assert_eq!(encoding, RenderEncoding::TerminalAnsi);
                assert_eq!(error, None);
            }
            other => panic!("expected Welcome, got {other:?}"),
        }

        match server_event_rx
            .blocking_recv()
            .expect("client connected event")
        {
            ServerEvent::ClientConnected {
                client_id,
                cols,
                rows,
                cell_width_px,
                cell_height_px,
                pixel_mouse,
                writer,
            } => {
                assert_eq!(client_id, 42);
                assert_eq!((cols, rows), (100, 30));
                assert_eq!((cell_width_px, cell_height_px), (8, 16));
                assert!(pixel_mouse);
                #[cfg(unix)]
                assert_bounded_client_writer_socket(&writer);
                drop(writer);
            }
            other => panic!("expected ClientConnected, got {other:?}"),
        }

        drop(client_stream);
        should_quit.store(true, Ordering::Release);
        handle
            .join()
            .expect("handshake thread join")
            .expect("handshake thread result");
    }

    #[cfg(unix)]
    fn assert_bounded_client_writer_socket(writer: &ClientWriter) {
        use std::os::fd::AsRawFd as _;
        let guard = writer.render.queue.disconnect.lock().unwrap_or_else(|p| p.into_inner());
        let LocalStream::UdSocket(socket) = guard.as_ref().expect("registered writer socket");
        let mut bytes: libc::c_int = 0;
        let mut len = std::mem::size_of_val(&bytes) as libc::socklen_t;
        assert_eq!(unsafe {
            libc::getsockopt(
                socket.inner().as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF,
                (&mut bytes as *mut libc::c_int).cast(), &mut len,
            )
        }, 0);
        #[cfg(target_os = "linux")]
        assert_eq!(bytes, 2 * CLIENT_SEND_BUFFER_BYTES, "Linux reports twice the requested send budget");
        #[cfg(not(target_os = "linux"))]
        assert!(bytes > 0 && bytes <= 2 * CLIENT_SEND_BUFFER_BYTES, "bounded Unix send buffer: {bytes}");
        assert_eq!(socket.inner().write_timeout().unwrap(), None,
            "normal writers must not disconnect regularly slow readers");
    }

    #[cfg(unix)]
    #[test]
    fn shell_handshake_bounds_kernel_backlog_without_truncating_large_frame() {
        let (mut client, server, _path) = local_stream_pair("bounded-writer-handshake");
        client.set_recv_timeout(Some(Duration::from_secs(5))).unwrap();
        let (events, mut event_rx) = mpsc::channel(8);
        let should_quit = Arc::new(AtomicBool::new(false));
        let reader_quit = should_quit.clone();
        let reader = std::thread::spawn(move || {
            handle_client_handshake(server, 91, &events, &reader_quit)
        });
        protocol::write_message(&mut client, &endpoint_hello(80, 24)).unwrap();
        let welcome: ServerMessage = protocol::read_message(&mut client, MAX_FRAME_SIZE).unwrap();
        assert!(endpoint_welcome(welcome).error.is_none());
        let ServerEvent::ClientShellConnected { writer, .. } = event_rx.blocking_recv().unwrap() else {
            panic!("real shell writer must be registered");
        };
        assert_bounded_client_writer_socket(&writer);
        let message = ServerMessage::Clipboard { data: "YWFh".repeat(256 * 1024) };
        let framed = frame_server_message(&message);
        assert!(framed.len() > 100 * CLIENT_SEND_BUFFER_BYTES as usize);
        assert!(framed.len() <= MAX_FRAME_SIZE);
        writer.control.send(framed).unwrap();
        let delivered: ServerMessage = protocol::read_message(&mut client, MAX_FRAME_SIZE).unwrap();
        assert_eq!(delivered, message, "frame much larger than socket budget must arrive intact");
        drop(writer);
        drop(client);
        should_quit.store(true, Ordering::Release);
        reader.join().unwrap().unwrap();
    }

    #[test]
    fn shell_handshake_selects_snapshot_delta_only_when_explicitly_offered() {
        for offered in [false, true] {
            let (mut client, server, _path) = local_stream_pair("snapshot-codec-handshake");
            client.set_recv_timeout(Some(Duration::from_secs(2))).unwrap();
            let (events, mut event_rx) = mpsc::channel(4);
            let quit = Arc::new(AtomicBool::new(false));
            let reader_quit = quit.clone();
            let worker = std::thread::spawn(move || {
                handle_client_handshake(server, 94, &events, &reader_quit)
            });
            let ClientMessage::EndpointControl { kind, data } = endpoint_hello(80, 24) else { panic!() };
            let mut hello: EndpointClientHello = serde_json::from_str(&data).unwrap();
            if offered {
                hello.snapshot_codecs.push(protocol::endpoint::SNAPSHOT_DELTA_CODEC_V1.into());
            }
            protocol::write_message(&mut client, &ClientMessage::EndpointControl {
                kind, data: serde_json::to_string(&hello).unwrap(),
            }).unwrap();
            let welcome = endpoint_welcome(protocol::read_message(&mut client, MAX_FRAME_SIZE).unwrap());
            assert_eq!(welcome.generation, ENDPOINT_PROTOCOL_GENERATION);
            assert_eq!(welcome.snapshot_codec, if offered {
                protocol::endpoint::SNAPSHOT_DELTA_CODEC_V1
            } else {
                protocol::endpoint::SNAPSHOT_CODEC_V1
            });
            let Some(ServerEvent::ClientShellConnected { writer, .. }) = event_rx.blocking_recv()
            else { panic!("handshake must register a shell writer") };
            assert_eq!(writer.render.queue.snapshot_delta, offered);
            drop(writer);
            drop(client);
            quit.store(true, Ordering::Release);
            worker.join().unwrap().unwrap();
        }
    }

    #[test]
    fn dedicated_client_shell_handshake_uses_surface_viewport() {
        let (mut client_stream, server_stream, _path) = local_stream_pair("client-shell-handshake");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let handshake_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            handle_client_handshake(server_stream, 43, &server_event_tx, &handshake_quit)
        });

        protocol::write_message(&mut client_stream, &endpoint_hello(80, 29))
            .expect("write shell hello");

        let welcome: ServerMessage =
            protocol::read_message(&mut client_stream, MAX_FRAME_SIZE).expect("read welcome");
        let welcome = endpoint_welcome(welcome);
        assert_eq!(welcome.generation, ENDPOINT_PROTOCOL_GENERATION);
        assert!(welcome.error.is_none());
        match server_event_rx
            .blocking_recv()
            .expect("client shell connected event")
        {
            ServerEvent::ClientShellConnected {
                client_id,
                surface_cols,
                surface_rows,
                cell_width_px,
                cell_height_px,
                pixel_mouse,
                direct_graphics,
                endpoint_keybindings,
                mouse_capture,
                surface_active,
                surface_reuse,
                surface_delta,
                surface_scroll,
                writer,
            } => {
                assert!(!surface_reuse);
                assert!(!surface_delta);
                assert!(!surface_scroll);
                assert_eq!(client_id, 43);
                assert_eq!((surface_cols, surface_rows), (80, 29));
                assert_eq!((cell_width_px, cell_height_px), (8, 16));
                assert!(pixel_mouse);
                assert!(direct_graphics);
                assert!(endpoint_keybindings);
                assert!(mouse_capture);
                assert!(surface_active);
                drop(writer);
            }
            other => panic!("expected ClientShellConnected, got {other:?}"),
        }

        drop(client_stream);
        should_quit.store(true, Ordering::Release);
        handle
            .join()
            .expect("handshake thread join")
            .expect("handshake thread result");
    }

    #[test]
    fn endpoint_health_waits_for_registration_but_not_the_initial_snapshot() {
        let (mut client, server, _path) = local_stream_pair("startup-health");
        let (events, mut pending) = mpsc::channel(1);
        events
            .blocking_send(ServerEvent::ClientWriterDrained { client_id: 99 })
            .unwrap();
        let server_events = events.clone();
        let server_thread = std::thread::spawn(move || {
            handle_client_handshake(
                server,
                43,
                &server_events,
                &Arc::new(AtomicBool::new(false)),
            )
        });
        let mut reader = client.try_clone().unwrap();
        let (messages, received) = std::sync::mpsc::channel();
        let client_thread = std::thread::spawn(move || {
            for _ in 0..3 {
                let message: ServerMessage =
                    protocol::read_message(&mut reader, MAX_FRAME_SIZE).unwrap();
                messages.send(message).unwrap();
            }
        });
        protocol::write_message(&mut client, &endpoint_hello(80, 29)).unwrap();
        let welcome = endpoint_welcome(received.recv_timeout(Duration::from_secs(2)).unwrap());
        assert!(welcome.error.is_none());
        let ping = ClientMessage::EndpointControl {
            kind: crate::protocol::endpoint::HEALTH_PING_KIND.into(),
            data: "startup".into(),
        };
        protocol::write_message(&mut client, &ping).unwrap();
        assert!(matches!(
            received.recv_timeout(Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(matches!(
            pending.blocking_recv(),
            Some(ServerEvent::ClientWriterDrained { client_id: 99 })
        ));
        let Some(ServerEvent::ClientShellConnected { writer, .. }) = pending.blocking_recv() else {
            panic!("registration must precede other client events");
        };
        assert!(matches!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            ServerMessage::EndpointControl { kind, .. }
                if kind == crate::protocol::endpoint::HEALTH_PONG_KIND
        ));

        // Registered transports answer health checks while the state loop is still busy.
        events
            .blocking_send(ServerEvent::ClientWriterDrained { client_id: 99 })
            .unwrap();
        protocol::write_message(&mut client, &ping).unwrap();
        assert!(matches!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            ServerMessage::EndpointControl { kind, .. }
                if kind == crate::protocol::endpoint::HEALTH_PONG_KIND
        ));
        protocol::write_message(&mut client, &ClientMessage::Detach).unwrap();
        assert!(matches!(
            pending.blocking_recv(),
            Some(ServerEvent::ClientWriterDrained { client_id: 99 })
        ));
        assert!(matches!(
            pending.blocking_recv(),
            Some(ServerEvent::ClientDetach { client_id: 43 })
        ));
        server_thread.join().unwrap().unwrap();
        client_thread.join().unwrap();
        drop(writer);
    }

    #[test]
    fn dedicated_client_shell_handshake_rejects_empty_surface() {
        let (mut client_stream, server_stream, _path) =
            local_stream_pair("client-shell-empty-surface");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let handshake_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            handle_client_handshake(server_stream, 43, &server_event_tx, &handshake_quit)
        });

        protocol::write_message(&mut client_stream, &endpoint_hello(0, 29))
            .expect("write empty shell hello");

        let welcome: ServerMessage =
            protocol::read_message(&mut client_stream, MAX_FRAME_SIZE).expect("read welcome");
        let welcome = endpoint_welcome(welcome);
        assert!(welcome
            .error
            .is_some_and(|error| error.message.contains("non-empty pane surface")));
        handle
            .join()
            .expect("handshake thread join")
            .expect("handshake thread result");
        assert!(server_event_rx.try_recv().is_err());
    }

    #[test]
    fn client_read_loop_stops_after_detach() {
        let (mut client_stream, server_stream, _path) = local_stream_pair("client-read-detach");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let read_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            client_read_loop(server_stream, 7, &server_event_tx, &read_quit)
        });

        let mut messages = Vec::new();
        protocol::write_message(&mut messages, &ClientMessage::Detach).unwrap();
        protocol::write_message(
            &mut messages,
            &ClientMessage::ClipboardImage {
                target: crate::protocol::ClientClipboardImageTarget::DirectTerminal,
                extension: "png".into(),
                data: vec![1, 2, 3],
            },
        )
        .unwrap();
        client_stream
            .write_all(&messages)
            .expect("write detach and trailing message");

        assert!(matches!(
            recv_server_event(&mut server_event_rx, "detach event"),
            ServerEvent::ClientDetach { client_id: 7 }
        ));
        handle
            .join()
            .expect("read thread join")
            .expect("read thread result");
        assert!(server_event_rx.try_recv().is_err());
    }

    #[test]
    fn client_read_loop_ignores_unknown_endpoint_control() {
        let (mut client_stream, server_stream, _path) =
            local_stream_pair("client-read-future-control");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let read_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            client_read_loop(server_stream, 7, &server_event_tx, &read_quit)
        });

        protocol::write_message(
            &mut client_stream,
            &ClientMessage::EndpointControl {
                kind: "future.optional.v1".into(),
                data: "{}".into(),
            },
        )
        .unwrap();
        protocol::write_message(&mut client_stream, &ClientMessage::Detach).unwrap();

        assert!(matches!(
            recv_server_event(&mut server_event_rx, "detach after future control"),
            ServerEvent::ClientDetach { client_id: 7 }
        ));
        handle
            .join()
            .expect("read thread join")
            .expect("read thread result");
    }

    #[test]
    fn client_read_loop_closes_on_unsafe_shell_resize() {
        let (mut client_stream, server_stream, _path) =
            local_stream_pair("client-read-unsafe-resize");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let read_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            client_read_loop(server_stream, 7, &server_event_tx, &read_quit)
        });

        protocol::write_message(
            &mut client_stream,
            &ClientMessage::ClientShellResize {
                cell_width_px: 8,
                cell_height_px: 16,
                surface_size: crate::protocol::ClientSurfaceSize {
                    cols: MAX_CLIENT_SHELL_DIMENSION,
                    rows: MAX_CLIENT_SHELL_DIMENSION,
                },
                pixel_mouse: false,
            },
        )
        .unwrap();

        assert!(matches!(
            recv_server_event(&mut server_event_rx, "unsafe resize disconnect"),
            ServerEvent::ClientDisconnected { client_id: 7 }
        ));
        handle
            .join()
            .expect("read thread join")
            .expect("read thread result");
    }

    #[test]
    fn client_read_loop_rejects_oversized_bracketed_paste_without_disconnect() {
        let (mut client_stream, server_stream, _path) = local_stream_pair("client-read-oversized");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let read_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            client_read_loop(server_stream, 7, &server_event_tx, &read_quit)
        });

        protocol::write_message(
            &mut client_stream,
            &ClientMessage::Input {
                data: bracketed_paste_with_total_len(MAX_INPUT_PAYLOAD),
            },
        )
        .expect("write maximum-size bracketed paste");

        match recv_server_event(&mut server_event_rx, "maximum-size paste event") {
            ServerEvent::ClientInput { client_id, data } => {
                assert_eq!(client_id, 7);
                assert_eq!(data.len(), MAX_INPUT_PAYLOAD);
            }
            other => panic!("expected maximum-size ClientInput, got {other:?}"),
        }

        protocol::write_message(
            &mut client_stream,
            &ClientMessage::Input {
                data: bracketed_paste_with_total_len(MAX_INPUT_PAYLOAD + 1),
            },
        )
        .expect("write oversized bracketed paste");

        match recv_server_event(&mut server_event_rx, "oversized paste rejection") {
            ServerEvent::ClientPasteRejected {
                client_id,
                size,
                max,
            } => {
                assert_eq!(client_id, 7);
                assert_eq!(size, MAX_INPUT_PAYLOAD + 1);
                assert_eq!(max, MAX_INPUT_PAYLOAD);
            }
            ServerEvent::ClientDisconnected { .. } => {
                panic!("oversized input must be rejected without disconnecting the client")
            }
            other => panic!("expected ClientPasteRejected, got {other:?}"),
        }

        protocol::write_message(
            &mut client_stream,
            &ClientMessage::Input {
                data: b"still connected".to_vec(),
            },
        )
        .expect("write valid input after rejection");

        match recv_server_event(&mut server_event_rx, "valid input after rejection") {
            ServerEvent::ClientInput { client_id, data } => {
                assert_eq!(client_id, 7);
                assert_eq!(data, b"still connected");
            }
            other => panic!("expected ClientInput after rejection, got {other:?}"),
        }

        drop(client_stream);
        should_quit.store(true, Ordering::Release);
        handle
            .join()
            .expect("read thread join")
            .expect("read thread result");
    }

    #[test]
    fn client_read_loop_disconnects_oversized_non_paste_input() {
        let (mut client_stream, server_stream, _path) =
            local_stream_pair("client-read-oversized-non-paste");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let read_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            client_read_loop(server_stream, 7, &server_event_tx, &read_quit)
        });

        protocol::write_message(
            &mut client_stream,
            &ClientMessage::Input {
                data: vec![b'x'; MAX_INPUT_PAYLOAD + 1],
            },
        )
        .expect("write oversized non-paste input");

        assert!(matches!(
            recv_server_event(&mut server_event_rx, "oversized non-paste disconnect"),
            ServerEvent::ClientDisconnected { client_id: 7 }
        ));

        drop(client_stream);
        should_quit.store(true, Ordering::Release);
        handle
            .join()
            .expect("read thread join")
            .expect("read thread result");
    }

    #[test]
    fn client_read_loop_disconnects_marker_wrapped_invalid_utf8() {
        let (mut client_stream, server_stream, _path) =
            local_stream_pair("client-read-invalid-utf8-paste");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let read_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            client_read_loop(server_stream, 7, &server_event_tx, &read_quit)
        });
        let mut data = bracketed_paste_with_total_len(MAX_INPUT_PAYLOAD + 1);
        data[b"\x1b[200~".len()] = 0xff;

        protocol::write_message(&mut client_stream, &ClientMessage::Input { data })
            .expect("write marker-wrapped invalid UTF-8 input");

        assert!(matches!(
            recv_server_event(&mut server_event_rx, "invalid UTF-8 input disconnect"),
            ServerEvent::ClientDisconnected { client_id: 7 }
        ));

        drop(client_stream);
        should_quit.store(true, Ordering::Release);
        handle
            .join()
            .expect("read thread join")
            .expect("read thread result");
    }

    #[test]
    fn client_read_loop_uses_authoritative_shell_resize_surface() {
        let (mut client_stream, server_stream, _path) = local_stream_pair("client-read-resize");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let read_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            client_read_loop(server_stream, 7, &server_event_tx, &read_quit)
        });

        protocol::write_message(
            &mut client_stream,
            &ClientMessage::ClientShellResize {
                cell_width_px: 8,
                cell_height_px: 16,
                surface_size: crate::protocol::ClientSurfaceSize { cols: 60, rows: 15 },
                pixel_mouse: true,
            },
        )
        .expect("write shell resize");
        assert!(matches!(
            recv_server_event(&mut server_event_rx, "shell resize"),
            ServerEvent::ClientShellResize {
                client_id: 7,
                surface_cols: 60,
                surface_rows: 15,
                cell_width_px: 8,
                cell_height_px: 16,
                pixel_mouse: true,
            }
        ));

        protocol::write_message(&mut client_stream, &ClientMessage::Detach).expect("write detach");
        assert!(matches!(
            recv_server_event(&mut server_event_rx, "detach event"),
            ServerEvent::ClientDetach { client_id: 7 }
        ));
        handle
            .join()
            .expect("read thread join")
            .expect("read thread result");
    }

    #[test]
    fn client_read_loop_keeps_single_host_theme_updates_ordered_and_palette_bounded() {
        let (mut client_stream, server_stream, _path) = local_stream_pair("client-read-host-theme");
        let (server_event_tx, mut server_event_rx) = mpsc::channel(4);
        let should_quit = Arc::new(AtomicBool::new(false));
        let read_quit = should_quit.clone();
        let handle = std::thread::spawn(move || {
            client_read_loop(server_stream, 7, &server_event_tx, &read_quit)
        });

        let colors = (0..=u8::MAX)
            .map(|index| {
                (
                    index,
                    crate::protocol::ClientHostColor {
                        r: index,
                        g: 0,
                        b: 0,
                    },
                )
            })
            .collect();
        protocol::write_message(
            &mut client_stream,
            &ClientMessage::ClientShellHostTheme {
                update: crate::protocol::ClientHostThemeUpdate::PaletteColors(colors),
            },
        )
        .expect("write bounded palette update");
        protocol::write_message(
            &mut client_stream,
            &ClientMessage::ClientShellHostTheme {
                update: crate::protocol::ClientHostThemeUpdate::Appearance(
                    crate::protocol::ClientHostAppearance::Dark,
                ),
            },
        )
        .expect("write ordered appearance update");

        assert!(matches!(
            recv_server_event(&mut server_event_rx, "bounded palette update"),
            ServerEvent::ClientShellHostTheme {
                client_id: 7,
                update: crate::protocol::ClientHostThemeUpdate::PaletteColors(colors),
            } if colors.len() == 256
        ));
        assert!(matches!(
            recv_server_event(&mut server_event_rx, "ordered appearance update"),
            ServerEvent::ClientShellHostTheme {
                client_id: 7,
                update: crate::protocol::ClientHostThemeUpdate::Appearance(
                    crate::protocol::ClientHostAppearance::Dark
                ),
            }
        ));

        let colors = vec![(0, crate::protocol::ClientHostColor { r: 0, g: 0, b: 0 },); 257];
        protocol::write_message(
            &mut client_stream,
            &ClientMessage::ClientShellHostTheme {
                update: crate::protocol::ClientHostThemeUpdate::PaletteColors(colors),
            },
        )
        .expect("write oversized palette update");
        assert!(matches!(
            recv_server_event(&mut server_event_rx, "oversized palette disconnect"),
            ServerEvent::ClientDisconnected { client_id: 7 }
        ));

        drop(client_stream);
        should_quit.store(true, Ordering::Release);
        handle
            .join()
            .expect("read thread join")
            .expect("read thread result");
    }

    #[test]
    fn pane_input_limits_charge_scroll_repeats() {
        let oversized_scroll = ClientPaneInputEvent::Mouse {
            kind: crate::protocol::ClientMouseKind::ScrollUp,
            position: crate::protocol::ClientMousePosition::Cell { column: 0, row: 0 },
            geometry: None,
            modifiers: 0,
            lines: (MAX_INPUT_EVENT_BATCH + 1) as u16,
        };
        assert_eq!(
            pane_input_event_limit(&[oversized_scroll]),
            InputEventLimit::TooManyEvents
        );
    }

    #[test]
    fn handshake_timeout_is_within_five_second_deadline() {
        // The handshake timeout must be short enough that
        // the connection is guaranteed to close within 5 seconds even with
        // OS overhead (thread scheduling, timer slack, cleanup).
        assert!(
            HANDSHAKE_TIMEOUT < Duration::from_secs(5),
            "HANDSHAKE_TIMEOUT ({:?}) must be less than 5 seconds to guarantee \
             connection close within the 5-second deadline",
            HANDSHAKE_TIMEOUT
        );
    }
}
