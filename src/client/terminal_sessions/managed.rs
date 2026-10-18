use super::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::protocol::endpoint::{
    terminal_session_geometry_valid, TerminalSessionMode, TerminalSessionRequest,
    TerminalSessionState, TERMINAL_SESSION_SET_MODE_KIND, TERMINAL_SESSION_STATE_KIND,
};

const UPGRADE_REQUIRED: &str = "managed terminal sessions require a matching Herdr CLI and server upgrade supporting terminal.session.set_mode/state";

#[derive(Debug)]
struct InputGate {
    target: String,
    mode: TerminalSessionMode,
    last_request_id: u64,
    pending: VecDeque<(u64, TerminalSessionMode)>,
    closed: bool,
}

impl InputGate {
    fn new(target: String) -> Self {
        Self {
            target,
            mode: TerminalSessionMode::Observe,
            last_request_id: 0,
            pending: VecDeque::from([(0, TerminalSessionMode::Observe)]),
            closed: false,
        }
    }

    fn request(&mut self, request: &TerminalSessionRequest) -> Result<(), String> {
        if self.closed || request.target != self.target {
            return Err("managed terminal session is closed or has a different target".into());
        }
        if request.request_id <= self.last_request_id {
            return Err("terminal session request_id must be positive and increase".into());
        }
        if !terminal_session_geometry_valid(request.cols, request.rows) {
            return Err("terminal session geometry exceeds the safe non-empty limit".into());
        }
        if self.pending.len() >= 16 {
            return Err("too many unacknowledged terminal session mode requests".into());
        }
        self.last_request_id = request.request_id;
        self.pending.push_back((request.request_id, request.mode));
        Ok(())
    }

    fn acknowledge(&mut self, state: &TerminalSessionState) -> Result<(), String> {
        let Some(&(request_id, mode)) = self.pending.front() else {
            return Err("unsolicited terminal session state".into());
        };
        if state.target != self.target
            || state.request_id != request_id
            || state.mode == TerminalSessionMode::Unknown
            || !terminal_session_geometry_valid(state.cols, state.rows)
            || (state.error.is_none() && state.mode != mode)
            || (state.error.is_some() && state.mode != self.mode)
        {
            return Err(
                "terminal session state does not match the pending request and target".into(),
            );
        }
        self.pending.pop_front();
        self.mode = state.mode;
        Ok(())
    }

    fn writable(&self) -> bool {
        !self.closed && self.pending.is_empty() && self.mode == TerminalSessionMode::Control
    }
}

/// Keeps the matching native connection alive while the backend changes its lease.
pub fn run_terminal_session_managed(target: String, cols: u16, rows: u16) -> io::Result<()> {
    if !terminal_session_geometry_valid(cols, rows) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsafe managed terminal geometry",
        ));
    }
    let mut stream =
        connect_terminal_session_stream(target.clone(), cols, rows, "managing terminal session")?;
    let initial = TerminalSessionRequest {
        target: target.clone(),
        mode: TerminalSessionMode::Observe,
        cols,
        rows,
        request_id: 0,
    };
    write_mode_request(&mut stream, &initial)?;
    // Unknown optional controls on older servers are ignored. Bound only this
    // feature negotiation, using the existing partial-read-preserving reader;
    // ordinary frames and all subsequent transitions have no read deadline.
    crate::ipc::set_local_stream_polling(&mut stream, true)?;
    let stopped = AtomicBool::new(false);
    let first = protocol::read_message::<_, ServerMessage>(
        &mut super::super::transport::EndpointReader {
            stream: &mut stream,
            stopped: &stopped,
            deadline: Some(Instant::now() + super::super::handshake::LOCAL_HANDSHAKE_READ_TIMEOUT),
        },
        MAX_GRAPHICS_FRAME_SIZE,
    )
    .map_err(|error| io::Error::other(format!("{UPGRADE_REQUIRED}: {error}")))?;
    crate::ipc::set_local_stream_polling(&mut stream, false)?;
    let state = decode_state(first)
        .map_err(|error| io::Error::other(format!("{UPGRADE_REQUIRED}: {error}")))?;
    let mut gate = InputGate::new(target);
    gate.acknowledge(&state).map_err(io::Error::other)?;
    emit_state(&state, &mut io::stdout().lock())?;
    if let Some(error) = state.error {
        return Err(io::Error::other(error));
    }

    let shared = Arc::new(Mutex::new((gate, stream.try_clone()?)));
    let input = shared.clone();
    std::thread::Builder::new()
        .name("terminal-managed-input".into())
        .spawn(move || {
            managed_input_loop(io::stdin().lock(), &input);
        })?;

    let result = managed_output(&mut stream, &shared, &mut io::stdout().lock());
    finish_managed_input(&shared);
    result
}

fn managed_input_loop(stdin: impl io::BufRead, shared: &Mutex<(InputGate, LocalStream)>) {
    for line in stdin.lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let error = {
            let mut session = shared.lock().unwrap_or_else(|poison| poison.into_inner());
            let (gate, stream) = &mut *session;
            match managed_input(gate, &line) {
                Ok(message) => {
                    let release = matches!(message, ClientMessage::Detach);
                    if write_to_server(stream, &message).is_err() || release {
                        gate.closed = true;
                        return;
                    }
                    None
                }
                Err(error) => Some(error),
            }
        };
        if let Some(error) = error {
            eprintln!("herdr: terminal session managed input ignored: {error}");
        }
    }
    finish_managed_input(shared);
}

fn finish_managed_input(shared: &Mutex<(InputGate, LocalStream)>) {
    let mut session = shared.lock().unwrap_or_else(|poison| poison.into_inner());
    session.0.closed = true;
    let _ = write_to_server(&mut session.1, &ClientMessage::Detach);
}

fn publish_managed_state(
    shared: &Mutex<(InputGate, LocalStream)>,
    state: &TerminalSessionState,
    stdout: &mut impl io::Write,
) -> io::Result<()> {
    {
        let mut session = shared.lock().unwrap_or_else(|poison| poison.into_inner());
        session.0.acknowledge(state).map_err(io::Error::other)?;
    }
    // Authority comes from the backend reply, not stdout flush completion.
    // Never retain the cleanup/native-write mutex across blocked output.
    emit_state(state, stdout)
}

fn write_mode_request(
    stream: &mut LocalStream,
    request: &TerminalSessionRequest,
) -> io::Result<()> {
    write_to_server(
        stream,
        &ClientMessage::EndpointControl {
            kind: TERMINAL_SESSION_SET_MODE_KIND.into(),
            data: serde_json::to_string(request)?,
        },
    )
}

fn managed_input(gate: &mut InputGate, line: &str) -> Result<ClientMessage, String> {
    let value: serde_json::Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
    let mode = match value.get("type").and_then(|value| value.as_str()) {
        Some("terminal.acquire") => Some(TerminalSessionMode::Control),
        Some("terminal.observe") => Some(TerminalSessionMode::Observe),
        _ => None,
    };
    if let Some(mode) = mode {
        #[derive(serde::Deserialize)]
        struct ModeCommand {
            request_id: u64,
            cols: u16,
            rows: u16,
        }
        let command: ModeCommand =
            serde_json::from_value(value).map_err(|error| error.to_string())?;
        let request = TerminalSessionRequest {
            target: gate.target.clone(),
            mode,
            request_id: command.request_id,
            cols: command.cols,
            rows: command.rows,
        };
        gate.request(&request)?;
        return Ok(ClientMessage::EndpointControl {
            kind: TERMINAL_SESSION_SET_MODE_KIND.into(),
            data: serde_json::to_string(&request).map_err(|error| error.to_string())?,
        });
    }
    let command = terminal_control_command_from_json(line)?;
    if matches!(command, ClientMessage::Detach) {
        gate.closed = true;
        return Ok(command);
    }
    if let ClientMessage::Resize {
        cols,
        rows,
        cell_width_px,
        cell_height_px,
        ..
    } = &command
    {
        if !terminal_session_geometry_valid(*cols, *rows)
            || *cell_width_px > 4096
            || *cell_height_px > 4096
        {
            return Err("terminal.resize exceeds the safe geometry limit".into());
        }
    }
    if gate.writable() {
        Ok(command)
    } else {
        Err("terminal input requires acknowledged control with no pending transition".into())
    }
}

fn decode_state(message: ServerMessage) -> Result<TerminalSessionState, String> {
    match message {
        ServerMessage::EndpointControl { kind, data } if kind == TERMINAL_SESSION_STATE_KIND => {
            serde_json::from_str(&data)
                .map_err(|error| format!("invalid terminal session state: {error}"))
        }
        ServerMessage::ServerShutdown { reason } => {
            Err(reason.unwrap_or_else(|| "server closed managed session".into()))
        }
        _ => Err("server did not acknowledge managed terminal session".into()),
    }
}

fn emit_state(state: &TerminalSessionState, stdout: &mut impl io::Write) -> io::Result<()> {
    let mode = match state.mode {
        TerminalSessionMode::Observe => "observe",
        TerminalSessionMode::Control => "control",
        TerminalSessionMode::Unknown => {
            return Err(io::Error::other("unknown terminal session state mode"))
        }
    };
    info!(event = "terminal.session.state", pid = std::process::id(), target = %state.target,
        mode, request_id = state.request_id, error = ?state.error, "terminal session state acknowledged");
    let mut value = serde_json::to_value(state)?;
    value["type"] = "terminal.state".into();
    serde_json::to_writer(&mut *stdout, &value)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}

fn managed_output(
    stream: &mut LocalStream,
    shared: &Mutex<(InputGate, LocalStream)>,
    stdout: &mut impl io::Write,
) -> io::Result<()> {
    loop {
        match protocol::read_message::<_, ServerMessage>(stream, MAX_GRAPHICS_FRAME_SIZE) {
            Ok(message @ ServerMessage::EndpointControl { .. }) => {
                if !matches!(&message, ServerMessage::EndpointControl { kind, .. } if kind == TERMINAL_SESSION_STATE_KIND)
                {
                    continue;
                }
                let state = decode_state(message).map_err(io::Error::other)?;
                publish_managed_state(shared, &state, stdout)?;
            }
            Ok(ServerMessage::Terminal(frame)) => write_terminal_frame(stdout, &frame)?,
            Ok(ServerMessage::ServerShutdown { reason }) => {
                serde_json::to_writer(
                    &mut *stdout,
                    &serde_json::json!({"type":"terminal.closed", "reason":reason}),
                )?;
                stdout.write_all(b"\n")?;
                stdout.flush()?;
                return Ok(());
            }
            Ok(_) => {}
            Err(protocol::FramingError::UnexpectedEof) => return Ok(()),
            Err(error) => return Err(io::Error::other(error.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(
        request_id: u64,
        mode: TerminalSessionMode,
        error: Option<&str>,
    ) -> TerminalSessionState {
        TerminalSessionState {
            target: "w1:p1".into(),
            request_id,
            mode,
            cols: 80,
            rows: 24,
            error: error.map(str::to_owned),
        }
    }

    fn initialized() -> InputGate {
        let mut gate = InputGate::new("w1:p1".into());
        gate.acknowledge(&state(0, TerminalSessionMode::Observe, None))
            .unwrap();
        gate
    }

    #[test]
    fn managed_cli_gates_real_input_until_matching_control_ack() {
        let mut gate = initialized();
        let commands = [
            r#"{"type":"terminal.input","text":"real"}"#,
            r#"{"type":"terminal.input","bytes":"Axs="}"#,
            r#"{"type":"terminal.resize","cols":100,"rows":30}"#,
            r#"{"type":"terminal.mouse","action":"down","column":1,"row":2}"#,
            r#"{"type":"terminal.scroll","direction":"up","lines":2}"#,
        ];
        for command in commands {
            assert!(managed_input(&mut gate, command).is_err());
        }
        let acquire = managed_input(
            &mut gate,
            r#"{"type":"terminal.acquire","request_id":1,"cols":80,"rows":24}"#,
        )
        .unwrap();
        let ClientMessage::EndpointControl { kind, data } = acquire else {
            panic!("native control envelope")
        };
        assert_eq!(kind, TERMINAL_SESSION_SET_MODE_KIND);
        let request: TerminalSessionRequest = serde_json::from_str(&data).unwrap();
        assert_eq!(
            (request.target.as_str(), request.mode, request.request_id),
            ("w1:p1", TerminalSessionMode::Control, 1)
        );
        for command in commands {
            assert!(managed_input(&mut gate, command).is_err());
        }
        gate.acknowledge(&state(1, TerminalSessionMode::Control, None))
            .unwrap();
        for command in commands {
            assert!(managed_input(&mut gate, command).is_ok());
        }
        assert!(managed_input(
            &mut gate,
            r#"{"type":"terminal.resize","cols":80,"rows":24,"cell_width_px":4294967295}"#
        )
        .is_err());
        assert_eq!(
            managed_input(&mut gate, r#"{"type":"terminal.input","bytes":"Axs="}"#).unwrap(),
            ClientMessage::Input { data: vec![3, 27] },
        );
    }

    #[test]
    fn managed_cli_old_control_ack_cannot_flush_after_newer_observe_request() {
        let mut gate = initialized();
        managed_input(
            &mut gate,
            r#"{"type":"terminal.acquire","request_id":1,"cols":80,"rows":24}"#,
        )
        .unwrap();
        managed_input(
            &mut gate,
            r#"{"type":"terminal.observe","request_id":2,"cols":120,"rows":40}"#,
        )
        .unwrap();
        gate.acknowledge(&state(1, TerminalSessionMode::Control, None))
            .unwrap();
        assert!(!gate.writable());
        assert!(managed_input(
            &mut gate,
            r#"{"type":"terminal.input","text":"must not reach app"}"#
        )
        .is_err());
        gate.acknowledge(&state(2, TerminalSessionMode::Observe, None))
            .unwrap();
        assert!(!gate.writable());
        assert!(gate
            .acknowledge(&state(1, TerminalSessionMode::Control, None))
            .is_err());
        assert!(!gate.writable());
    }

    #[test]
    fn managed_cli_denied_control_never_authorizes_input_and_retry_is_explicit() {
        let mut gate = initialized();
        managed_input(
            &mut gate,
            r#"{"type":"terminal.acquire","request_id":1,"cols":80,"rows":24}"#,
        )
        .unwrap();
        gate.acknowledge(&state(
            1,
            TerminalSessionMode::Observe,
            Some("already attached"),
        ))
        .unwrap();
        assert!(!gate.writable());
        assert!(managed_input(
            &mut gate,
            r#"{"type":"terminal.acquire","request_id":1,"cols":80,"rows":24}"#
        )
        .is_err());
        assert!(managed_input(
            &mut gate,
            r#"{"type":"terminal.acquire","request_id":2,"cols":4096,"rows":4096}"#
        )
        .is_err());
        managed_input(
            &mut gate,
            r#"{"type":"terminal.acquire","request_id":2,"cols":80,"rows":24}"#,
        )
        .unwrap();
        let mut wrong_target = state(2, TerminalSessionMode::Control, None);
        wrong_target.target = "w1:p2".into();
        assert!(gate.acknowledge(&wrong_target).is_err());
        assert!(!gate.writable());
        assert!(gate
            .acknowledge(&state(3, TerminalSessionMode::Control, None))
            .is_err());
        gate.acknowledge(&state(2, TerminalSessionMode::Control, None))
            .unwrap();
        assert!(gate.writable());
        assert_eq!(
            managed_input(&mut gate, r#"{"type":"terminal.release"}"#).unwrap(),
            ClientMessage::Detach
        );
        assert!(!gate.writable());
    }

    #[test]
    fn managed_cli_rejects_unacknowledged_or_incompatible_initial_stream() {
        assert!(
            decode_state(ServerMessage::Terminal(protocol::TerminalFrame {
                seq: 1,
                width: 80,
                height: 24,
                full: true,
                bytes: b"not an ack".to_vec(),
            }))
            .is_err()
        );
        let mut gate = InputGate::new("w1:p1".into());
        assert!(gate
            .acknowledge(&state(0, TerminalSessionMode::Control, None))
            .is_err());
        assert!(!gate.writable());
        gate.acknowledge(&state(0, TerminalSessionMode::Observe, None))
            .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn managed_cli_backend_ack_allows_input_and_cleanup_while_stdout_is_blocked() {
        use interprocess::local_socket::traits::Listener as _;
        use std::io::{BufReader, Write};
        use std::os::unix::net::UnixStream;
        use std::sync::mpsc;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        struct NotifyWriter {
            stream: UnixStream,
            entered: Option<mpsc::Sender<()>>,
        }
        impl io::Write for NotifyWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if let Some(entered) = self.entered.take() {
                    entered.send(()).unwrap();
                }
                self.stream.write(bytes)
            }
            fn flush(&mut self) -> io::Result<()> {
                self.stream.flush()
            }
        }

        for explicit_release in [false, true] {
            let path = std::path::PathBuf::from(format!(
                "/tmp/herdr-managed-eof-{}-{}.sock",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            ));
            let listener = crate::ipc::bind_local_listener(&path).unwrap();
            let native = crate::ipc::connect_local_stream(&path).unwrap();
            let mut backend = listener.accept().unwrap();
            backend
                .set_recv_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = native.try_clone().unwrap();
            let mut gate = initialized();
            managed_input(
                &mut gate,
                r#"{"type":"terminal.acquire","request_id":1,"cols":80,"rows":24}"#,
            )
            .unwrap();
            let shared = Arc::new(Mutex::new((gate, native)));
            let (mut stdin_writer, stdin_reader) = UnixStream::pair().unwrap();
            let input_shared = shared.clone();
            let input = std::thread::spawn(move || {
                managed_input_loop(BufReader::new(stdin_reader), &input_shared)
            });
            let (mut stdout_writer, stdout_reader) = UnixStream::pair().unwrap();
            // Fill a real output transport without closing its read FD. The
            // state emitter must block on OS backpressure, not a mock response.
            stdout_writer.set_nonblocking(true).unwrap();
            loop {
                match stdout_writer.write(&[b'x'; 4096]) {
                    Ok(0) => panic!("output transport closed"),
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => panic!("{error}"),
                }
            }
            stdout_writer.set_nonblocking(false).unwrap();
            let (entered, waiting) = mpsc::channel();
            let output_shared = shared.clone();
            let output = std::thread::spawn(move || {
                managed_output(
                    &mut reader,
                    &output_shared,
                    &mut NotifyWriter {
                        stream: stdout_writer,
                        entered: Some(entered),
                    },
                )
            });
            protocol::write_message(
                &mut backend,
                &ServerMessage::EndpointControl {
                    kind: TERMINAL_SESSION_STATE_KIND.into(),
                    data: serde_json::to_string(&state(1, TerminalSessionMode::Control, None))
                        .unwrap(),
                },
            )
            .unwrap();
            waiting.recv_timeout(Duration::from_secs(2)).unwrap();
            stdin_writer
                .write_all(b"{\"type\":\"terminal.input\",\"text\":\"after real backend ack\"}\n")
                .unwrap();
            if explicit_release {
                stdin_writer
                    .write_all(b"{\"type\":\"terminal.release\"}\n")
                    .unwrap();
            } else {
                drop(stdin_writer);
            }
            let forwarded =
                protocol::read_message::<_, ClientMessage>(&mut backend, MAX_GRAPHICS_FRAME_SIZE);
            let detach =
                protocol::read_message::<_, ClientMessage>(&mut backend, MAX_GRAPHICS_FRAME_SIZE);
            // Only now unblock stdout. Detach must have arrived while its read
            // FD was still open and nobody drained the blocked acknowledgement.
            drop(stdout_reader);
            input.join().unwrap();
            assert!(output.join().unwrap().is_err());
            assert!(
                matches!(forwarded, Ok(ClientMessage::Input { data }) if data == b"after real backend ack"),
                "input did not reach native stream while stdout was blocked"
            );
            assert!(matches!(detach, Ok(ClientMessage::Detach)), "{detach:?}");
            let session = shared.lock().unwrap_or_else(|poison| poison.into_inner());
            assert!(session.0.closed);
            assert!(!session.0.writable());
            drop(session);
            drop(backend);
            drop(shared);
            drop(listener);
            std::fs::remove_file(path).unwrap();
        }
    }
}
