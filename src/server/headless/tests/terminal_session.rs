use super::*;
use crate::protocol::endpoint::{TerminalSessionMode as Mode, TerminalSessionRequest, TerminalSessionState, TERMINAL_SESSION_STATE_KIND};

fn managed_client(server: &mut HeadlessServer, client_id: u64) -> ClientWriter {
    let writer = ClientWriter::test_paused();
    server.clients.insert(client_id, ClientConnection::new_with_mode(
        ClientConnectionMode::TerminalPending, (120, 40),
        crate::kitty_graphics::HostCellSize::default(), client_id,
        RenderEncoding::TerminalAnsi, Some(writer.clone()),
    ));
    writer
}

fn set_mode(server: &mut HeadlessServer, client_id: u64, target: &str, request_id: u64, mode: Mode, cols: u16, rows: u16) -> bool {
    server.handle_server_event(ServerEvent::ClientTerminalSessionMode {
        client_id,
        data: serde_json::to_string(&TerminalSessionRequest {
            target: target.into(), request_id, mode, cols, rows,
        }).unwrap(),
    })
}

fn messages(writer: &ClientWriter) -> Vec<ServerMessage> {
    writer.test_drain().into_iter().flat_map(|bytes| {
        let mut remaining = bytes.as_slice();
        let mut decoded = Vec::new();
        while !remaining.is_empty() {
            decoded.push(protocol::read_message::<_, ServerMessage>(&mut remaining, MAX_GRAPHICS_FRAME_SIZE).unwrap());
        }
        decoded
    }).collect()
}

fn state_and_full(writer: &ClientWriter, request_id: u64, mode: Mode, dimensions: (u16, u16), denied: bool) -> TerminalSessionState {
    let messages = messages(writer);
    assert_eq!(messages.len(), 2, "state and its full frame are one reliable boundary");
    let ServerMessage::EndpointControl { kind, data } = &messages[0] else { panic!("state first") };
    assert_eq!(kind, TERMINAL_SESSION_STATE_KIND);
    let state: TerminalSessionState = serde_json::from_str(data).unwrap();
    assert_eq!((state.request_id, state.mode, state.cols, state.rows), (request_id, mode, dimensions.0, dimensions.1));
    assert_eq!(state.error.is_some(), denied);
    let ServerMessage::Terminal(frame) = &messages[1] else { panic!("full frame after state") };
    assert!(frame.full);
    assert_eq!((frame.width, frame.height), dimensions);
    state
}

#[test]
fn managed_terminal_cycles_preserve_connection_binding_dimensions_and_release_lease() {
    with_terminal_session_test_server(|server, terminal_id, terminal, pane| {
        let writer = managed_client(server, 7);
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 120, 40));
        assert_eq!(state_and_full(&writer, 0, Mode::Observe, (80, 24), false).target, pane);
        assert!(server.terminal_attach_owners.is_empty());
        assert_eq!(server.app.terminal_runtimes.get(&terminal_id).unwrap().current_size(), (24, 80));
        for cycle in 0..3 {
            let acquire = 1 + cycle * 2;
            assert!(set_mode(server, 7, &pane, acquire, Mode::Control, 100, 30));
            state_and_full(&writer, acquire, Mode::Control, (100, 30), false);
            assert_eq!(server.terminal_attach_owners.get(&terminal), Some(&7));
            assert!(server.app.state.direct_attach_resize_locks.contains(&terminal_id));
            assert!(set_mode(server, 7, &pane, acquire + 1, Mode::Observe, 1, 1));
            state_and_full(&writer, acquire + 1, Mode::Observe, (100, 30), false);
            assert!(server.clients.contains_key(&7));
            assert!(server.terminal_attach_owners.is_empty());
            assert!(!server.app.state.direct_attach_resize_locks.contains(&terminal_id));
            assert_eq!(server.app.terminal_runtimes.get(&terminal_id).unwrap().current_size(), (30, 100));
        }
        // Observation tracks a later source resize, not the observer's request.
        server.app.terminal_runtimes.get(&terminal_id).unwrap().resize(27, 91, 0, 0);
        server.render_and_stream();
        let frames = messages(&writer);
        assert!(matches!(frames.last(), Some(ServerMessage::Terminal(frame)) if frame.width == 91 && frame.height == 27 && frame.full));
        assert_eq!(server.clients[&7].terminal_size, (91, 27));
    });
}

#[test]
fn managed_terminal_conflict_denial_preserves_owner_geometry_and_can_retry_after_eof() {
    with_terminal_session_test_server(|server, terminal_id, terminal, pane| {
        let first = managed_client(server, 7);
        let second = managed_client(server, 8);
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 120, 40));
        assert!(set_mode(server, 8, &pane, 0, Mode::Observe, 120, 40));
        messages(&first); messages(&second);
        assert!(set_mode(server, 7, &pane, 1, Mode::Control, 90, 28));
        state_and_full(&first, 1, Mode::Control, (90, 28), false);
        assert!(!set_mode(server, 8, &pane, 1, Mode::Control, 140, 50));
        let denied = state_and_full(&second, 1, Mode::Observe, (90, 28), true);
        assert!(denied.error.unwrap().contains("never take over"));
        assert_eq!(server.terminal_attach_owners.get(&terminal), Some(&7));
        assert_eq!(server.app.terminal_runtimes.get(&terminal_id).unwrap().current_size(), (28, 90));
        server.handle_server_event(ServerEvent::ClientDisconnected { client_id: 7 });
        assert!(!server.clients.contains_key(&7));
        assert!(server.terminal_attach_owners.is_empty());
        assert!(!server.app.state.direct_attach_resize_locks.contains(&terminal_id));
        assert!(set_mode(server, 8, &pane, 2, Mode::Control, 110, 32));
        state_and_full(&second, 2, Mode::Control, (110, 32), false);
        assert_eq!(server.terminal_attach_owners.get(&terminal), Some(&8));
        server.handle_server_event(ServerEvent::ClientDetach { client_id: 8 });
        assert!(server.terminal_attach_owners.is_empty());
        assert!(!server.app.state.direct_attach_resize_locks.contains(&terminal_id));
    });
}

#[test]
fn managed_terminal_denials_never_retarget_or_replay_a_transition() {
    with_terminal_session_test_server(|server, terminal_id, terminal, pane| {
        let writer = managed_client(server, 7);
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 80, 24));
        messages(&writer);
        assert!(!set_mode(server, 7, &terminal, 1, Mode::Control, 100, 30));
        assert_eq!(state_and_full(&writer, 1, Mode::Observe, (80, 24), true).target, pane);
        assert!(!set_mode(server, 7, &pane, 1, Mode::Control, 100, 30));
        state_and_full(&writer, 1, Mode::Observe, (80, 24), true);
        assert!(!set_mode(server, 7, &pane, 2, Mode::Control, 0, 30));
        state_and_full(&writer, 2, Mode::Observe, (80, 24), true);
        assert!(!set_mode(server, 7, &pane, 3, Mode::Control, 4096, 4096));
        state_and_full(&writer, 3, Mode::Observe, (80, 24), true);
        assert!(!set_mode(server, 7, &pane, 4, Mode::Unknown, 80, 24));
        state_and_full(&writer, 4, Mode::Observe, (80, 24), true);
        assert!(set_mode(server, 7, &pane, 5, Mode::Control, 80, 24));
        state_and_full(&writer, 5, Mode::Control, (80, 24), false);
        assert!(!set_mode(server, 7, &pane, 4, Mode::Observe, 80, 24));
        state_and_full(&writer, 4, Mode::Control, (80, 24), true);
        assert_eq!(server.terminal_attach_owners.get(&terminal), Some(&7));
        assert!(server.app.state.direct_attach_resize_locks.contains(&terminal_id));
    });
}

#[test]
fn managed_terminal_input_is_read_only_before_acquire_and_after_demote() {
    with_terminal_session_test_server(|server, terminal_id, _, pane| {
        let (runtime, mut input) = crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(
            80, 24, 100, b"\x1b[?1000h\x1b[?1006h", 16,
        );
        server.app.terminal_runtimes.insert(terminal_id.clone(), runtime);
        let writer = managed_client(server, 7);
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 80, 24));
        messages(&writer);
        let key = || ServerEvent::ClientInput { client_id: 7, data: b"\x03real-input".to_vec() };
        assert!(!server.handle_server_event(key()));
        assert!(input.try_recv().is_err());
        assert!(set_mode(server, 7, &pane, 1, Mode::Control, 80, 24));
        state_and_full(&writer, 1, Mode::Control, (80, 24), false);
        assert!(server.handle_server_event(key()));
        assert_eq!(input.try_recv().unwrap(), Bytes::from_static(b"\x03real-input"));
        assert!(set_mode(server, 7, &pane, 2, Mode::Observe, 1, 1));
        state_and_full(&writer, 2, Mode::Observe, (80, 24), false);
        assert!(!server.handle_server_event(key()));
        assert!(!server.handle_server_event(ServerEvent::ClientAttachMouse {
            client_id: 7, kind: protocol::ClientMouseKind::Down(protocol::ClientMouseButton::Left),
            position: protocol::ClientMousePosition::Cell { column: 1, row: 1 },
            geometry: None, modifiers: 0, lines: 1,
        }));
        assert!(!server.handle_server_event(ServerEvent::ClientAttachScroll {
            client_id: 7, source: AttachScrollSource::Wheel, direction: AttachScrollDirection::Up,
            lines: 1, column: None, row: None, modifiers: 0,
        }));
        assert!(!server.handle_server_event(ServerEvent::ClientResize {
            client_id: 7, cols: 1, rows: 1, cell_width_px: 0, cell_height_px: 0, pixel_mouse: false,
        }));
        assert_eq!(server.app.terminal_runtimes.get(&terminal_id).unwrap().current_size(), (24, 80));
        assert!(input.try_recv().is_err());
    });
}

#[test]
fn managed_terminal_malformed_request_closes_and_releases_owned_lease() {
    with_terminal_session_test_server(|server, terminal_id, _, pane| {
        let writer = managed_client(server, 7);
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 80, 24));
        assert!(set_mode(server, 7, &pane, 1, Mode::Control, 80, 24));
        messages(&writer);
        assert!(!server.handle_server_event(ServerEvent::ClientTerminalSessionMode { client_id: 7, data: "not json".into() }));
        assert!(!server.clients.contains_key(&7));
        assert!(server.terminal_attach_owners.is_empty());
        assert!(!server.app.state.direct_attach_resize_locks.contains(&terminal_id));
        assert!(matches!(messages(&writer).as_slice(), [ServerMessage::ServerShutdown { reason: Some(reason) }] if reason.contains("invalid terminal.session.set_mode")));
    });
}

#[test]
fn managed_terminal_transition_waits_for_synchronized_full_frame_without_delaying_release() {
    with_terminal_session_test_server(|server, terminal_id, _, pane| {
        let writer = managed_client(server, 7);
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 80, 24));
        messages(&writer);
        assert!(set_mode(server, 7, &pane, 1, Mode::Control, 80, 24));
        messages(&writer);
        server.app.terminal_runtimes.get(&terminal_id).unwrap().test_process_pty_bytes(b"\x1b[?2026hPARTIAL");
        assert!(set_mode(server, 7, &pane, 2, Mode::Observe, 120, 40));
        assert!(server.terminal_attach_owners.is_empty());
        assert_eq!(messages(&writer).len(), 1, "only the ack, never an uncommitted PTY grid");
        server.render_and_stream();
        assert!(messages(&writer).is_empty());
        server.app.terminal_runtimes.get(&terminal_id).unwrap().test_process_pty_bytes(b"COMPLETE\x1b[?2026l");
        server.render_and_stream();
        assert!(matches!(messages(&writer).as_slice(), [ServerMessage::Terminal(frame)] if frame.full));
    });
}

#[test]
fn managed_terminal_read_conflict_is_a_denial_not_disconnect_or_resize() {
    with_terminal_session_test_server(|server, terminal_id, _, pane| {
        let writer = managed_client(server, 7);
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 80, 24));
        messages(&writer);
        let (respond_to, _response_rx) = std::sync::mpsc::channel();
        server.pending_alt_screen_reads.push(
            crate::server::alt_screen_read::PendingAltScreenRead::start(
                terminal_id.clone(), "managed-read".into(), respond_to, "fallback".into(),
                api::schema::PaneReadResult {
                    pane_id: pane.clone(), workspace_id: "w1".into(), tab_id: "w1:t1".into(),
                    source: api::schema::ReadSource::Recent, format: api::schema::ReadFormat::Text,
                    text: String::new(), revision: 0, truncated: false,
                },
                120, false, crate::terminal::ScreenSnapshot { cols: 80, rows: Vec::new() },
                0, Instant::now(),
            ),
        );
        assert!(!set_mode(server, 7, &pane, 1, Mode::Control, 100, 30));
        let denied = state_and_full(&writer, 1, Mode::Observe, (80, 24), true);
        assert!(denied.error.unwrap().contains("read in progress"));
        assert!(server.clients.contains_key(&7));
        assert!(server.terminal_attach_owners.is_empty());
        assert_eq!(server.app.terminal_runtimes.get(&terminal_id).unwrap().current_size(), (24, 80));
        server.pending_alt_screen_reads.clear();
        assert!(set_mode(server, 7, &pane, 2, Mode::Control, 100, 30));
        state_and_full(&writer, 2, Mode::Control, (100, 30), false);
    });
}

#[test]
fn managed_terminal_binding_does_not_follow_a_reassigned_public_pane() {
    with_terminal_session_test_server(|server, original_id, _, pane| {
        let (original_runtime, mut original_input) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(80, 24, 0, b"", 4);
        server.app.terminal_runtimes.insert(original_id.clone(), original_runtime);
        let writer = managed_client(server, 7);
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 80, 24));
        messages(&writer);
        let workspace = &mut server.app.state.workspaces[0];
        let tab = workspace.test_add_tab(Some("other"));
        let other_pane = workspace.tabs[tab].root_pane;
        let other_id = workspace.terminal_id(other_pane).unwrap().clone();
        let original_pane = workspace.tabs[0].root_pane;
        workspace.tabs[0].panes.get_mut(&original_pane).unwrap().attached_terminal_id = other_id.clone();
        workspace.tabs[tab].panes.get_mut(&other_pane).unwrap().attached_terminal_id = original_id.clone();
        server.app.state.ensure_test_terminals();
        let (other_runtime, mut other_input) =
            crate::terminal::TerminalRuntime::test_with_channel_and_scrollback_bytes(80, 24, 0, b"", 4);
        server.app.terminal_runtimes.insert(other_id, other_runtime);
        assert!(set_mode(server, 7, &pane, 1, Mode::Control, 80, 24));
        state_and_full(&writer, 1, Mode::Control, (80, 24), false);
        assert!(server.handle_server_event(ServerEvent::ClientInput { client_id: 7, data: b"bound".to_vec() }));
        assert_eq!(original_input.try_recv().unwrap(), Bytes::from_static(b"bound"));
        assert!(other_input.try_recv().is_err(), "public alias changes must never redirect session input");
    });
}

#[test]
fn managed_terminal_initial_errors_are_acknowledged_without_acquiring_or_binding() {
    with_terminal_session_test_server(|server, _, _, pane| {
        let writer = managed_client(server, 7);
        assert!(!set_mode(server, 7, &pane, 1, Mode::Control, 100, 30));
        let reply = messages(&writer);
        let [ServerMessage::EndpointControl { kind, data }] = reply.as_slice() else { panic!("initial denial") };
        assert_eq!(kind, TERMINAL_SESSION_STATE_KIND);
        let denied: TerminalSessionState = serde_json::from_str(data).unwrap();
        assert_eq!(denied.mode, Mode::Observe);
        assert!(denied.error.unwrap().contains("must start in observe"));
        assert!(server.clients[&7].managed_terminal.is_none());
        assert!(server.terminal_attach_owners.is_empty());
        assert!(!set_mode(server, 7, "missing-target", 0, Mode::Observe, 80, 24));
        let reply = messages(&writer);
        let [ServerMessage::EndpointControl { data, .. }] = reply.as_slice() else { panic!("missing target denial") };
        let denied: TerminalSessionState = serde_json::from_str(data).unwrap();
        assert_eq!((denied.target.as_str(), denied.mode), ("missing-target", Mode::Observe));
        assert!(denied.error.unwrap().contains("not found"));
        assert!(server.clients[&7].managed_terminal.is_none());
        assert!(set_mode(server, 7, &pane, 0, Mode::Observe, 80, 24));
        state_and_full(&writer, 0, Mode::Observe, (80, 24), false);
    });
}
