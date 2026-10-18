use super::*;

fn receive_message(receiver: &std::sync::mpsc::Receiver<Vec<u8>>) -> (Vec<u8>, ServerMessage) {
    let bytes = receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("render message");
    let message = read_server_message(bytes.clone());
    (bytes, message)
}

fn decode_surface_message(
    decoder: &mut protocol::surface_reuse::Decoder,
    message: ServerMessage,
) -> crate::protocol::PaneSurfaceFrame {
    match decoder.decode(message).expect("decode surface message") {
        ServerMessage::PaneSurface(surface) => surface,
        other => panic!("expected decoded pane surface, got {other:?}"),
    }
}

fn without_asset_payload(
    mut surface: crate::protocol::PaneSurfaceFrame,
) -> crate::protocol::PaneSurfaceFrame {
    surface.graphics.assets.clear();
    surface
}

fn enable_delta(server: &mut HeadlessServer, client_id: u64) {
    server
        .clients
        .get_mut(&client_id)
        .expect("delta client")
        .render_state
        .enable_surface_delta(true);
}

fn fill_render_lane(server: &mut HeadlessServer, client_id: u64) -> ClientWriter {
    let queued = HeadlessServer::frame_server_message(&ServerMessage::ReloadSoundConfig)
        .expect("dummy frame");
    let paused = ClientWriter::test_paused();
    paused.test_fill_render(queued);
    server
        .clients
        .get_mut(&client_id)
        .unwrap()
        .writer
        .replace(paused)
        .unwrap()
}

#[tokio::test]
async fn surface_delta_reconstructs_metadata_text_and_hyperlinks() {
    let (mut server, _control_rx, render_rx, pane_id) =
        retained_test_server_with_control(b"initial text");
    enable_delta(&mut server, 1);
    server.render_and_stream();
    let (initial_bytes, initial_message) = receive_message(&render_rx);
    let mut decoder = protocol::surface_reuse::Decoder::new(true, false);
    let initial = decode_surface_message(&mut decoder, initial_message);
    assert_eq!(
        without_asset_payload(initial.clone()),
        without_asset_payload(
            server.clients[&1]
                .render_state
                .last_pane_surface()
                .expect("initial baseline")
                .clone()
        )
    );

    let initial_projection_revision = initial.projection_revision;
    server.app.state.workspaces[0].custom_name = Some("renamed workspace".into());
    write_shared_test_pane(
        &mut server,
        pane_id,
        b"\rupdated text \x1b]8;;https://example.test/path\x1b\\linked\x1b]8;;\x1b\\",
    );
    server.clients.get_mut(&1).unwrap().request_recompute();
    assert!(!server.render_retained_pane_surface_and_stream(&HashSet::from([pane_id])));
    server.render_and_stream();
    let (delta_bytes, delta_message) = receive_message(&render_rx);
    assert!(delta_bytes.len() < initial_bytes.len());
    assert!(matches!(
        &delta_message,
        ServerMessage::EndpointControl { kind, .. }
            if kind == protocol::surface_delta::MESSAGE_KIND
    ));
    let decoded = decode_surface_message(&mut decoder, delta_message);
    let committed = server.clients[&1]
        .render_state
        .last_pane_surface()
        .expect("updated baseline")
        .clone();
    assert_eq!(
        without_asset_payload(decoded.clone()),
        without_asset_payload(committed)
    );
    assert!(decoded.projection_revision > initial_projection_revision);
    assert!(frame_text(&decoded.frame).contains("updated text"));
    assert_eq!(decoded.frame.hyperlinks, vec!["https://example.test/path"]);
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn surface_delta_updates_global_popup_without_affecting_legacy_peer() {
    let mut server = test_headless_server();
    let mut workspace = crate::workspace::Workspace::test_new("popup-delta");
    let pane_id = workspace.focused_pane_id().expect("focused pane");
    workspace.insert_test_runtime(
        pane_id,
        crate::terminal::TerminalRuntime::test_with_screen_bytes(80, 23, b"base"),
    );
    server.app.state.workspaces = vec![workspace];
    server.app.state.active = Some(0);
    server.app.state.selected = 0;
    server.app.state.mode = crate::app::Mode::Terminal;
    let popup_runtime = crate::terminal::TerminalRuntime::test_with_screen_bytes(40, 12, b"POPUP");
    let (_, popup_terminal_id) = server.app.install_test_popup_runtime(popup_runtime);

    let (_delta_control, delta_render) = connect_matching_test_shell(&mut server, 1);
    let (_legacy_control, legacy_render) = connect_matching_test_shell(&mut server, 2);
    enable_delta(&mut server, 1);
    server.render_and_stream();
    let (initial_bytes, initial_message) = receive_message(&delta_render);
    let (_, _legacy_initial) = receive_message(&legacy_render);
    let mut decoder = protocol::surface_reuse::Decoder::new(true, false);
    let _ = decode_surface_message(&mut decoder, initial_message);

    server
        .app
        .terminal_runtimes
        .get(&popup_terminal_id)
        .expect("popup runtime")
        .test_process_pty_bytes(b"\rPOPUP updated");
    server.clients.get_mut(&1).unwrap().request_recompute();
    server.render_and_stream();

    let (delta_bytes, delta_message) = receive_message(&delta_render);
    assert!(delta_bytes.len() * 4 < initial_bytes.len());
    assert!(matches!(
        &delta_message,
        ServerMessage::EndpointControl { kind, .. }
            if kind == protocol::surface_delta::MESSAGE_KIND
    ));
    let decoded = decode_surface_message(&mut decoder, delta_message);
    assert!(decoded
        .popup
        .as_ref()
        .is_some_and(|popup| frame_text(&popup.frame).contains("POPUP updated")));

    let (_, legacy_message) = receive_message(&legacy_render);
    assert!(matches!(legacy_message, ServerMessage::PaneSurface(_)));
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn surface_delta_preserves_graphics_baseline_through_queue_recovery() {
    let (mut server, _control_rx, render_rx, pane_id) =
        retained_test_server_with_control(b"text before image");
    server.clients.get_mut(&1).unwrap().cell_size = crate::kitty_graphics::HostCellSize {
        width_px: 10,
        height_px: 20,
    };
    enable_delta(&mut server, 1);
    server.render_and_stream();
    let (initial_bytes, initial_message) = receive_message(&render_rx);
    let mut decoder = protocol::surface_reuse::Decoder::new(true, false);
    let _ = decode_surface_message(&mut decoder, initial_message);

    write_shared_test_pane(
        &mut server,
        pane_id,
        b"\x1b_Ga=T,f=32,t=d,i=7,p=3,s=1,v=1,c=1,r=1,q=2;/wAA/w==\x1b\\",
    );
    assert!(server.render_retained_pane_surface_and_stream(&HashSet::from([pane_id])));
    let (image_bytes, image_message) = receive_message(&render_rx);
    assert!(image_bytes.len() * 4 < initial_bytes.len());
    assert!(matches!(
        &image_message,
        ServerMessage::EndpointControl { kind, .. }
            if kind == protocol::surface_delta::MESSAGE_KIND
    ));
    let image = decode_surface_message(&mut decoder, image_message);
    assert!(!image.graphics.placements.is_empty());

    write_shared_test_pane(&mut server, pane_id, b"\rupdated text");
    assert!(server.render_retained_pane_surface_and_stream(&HashSet::from([pane_id])));
    let (text_bytes, text_message) = receive_message(&render_rx);
    assert!(text_bytes.len() * 4 < initial_bytes.len());
    assert!(matches!(
        &text_message,
        ServerMessage::EndpointControl { kind, .. }
            if kind == protocol::surface_delta::MESSAGE_KIND
    ));
    let text_update = decode_surface_message(&mut decoder, text_message);
    assert_eq!(text_update.graphics.placements, image.graphics.placements);
    assert!(text_update.graphics.assets.is_empty());

    let baseline_before_queue = server.clients[&1]
        .render_state
        .last_pane_surface()
        .expect("graphics baseline")
        .clone();
    let original_writer = fill_render_lane(&mut server, 1);
    write_shared_test_pane(
        &mut server,
        pane_id,
        b"\x1b_Ga=T,f=32,t=d,i=8,p=3,s=1,v=1,c=1,r=1,q=2;AP8A/w==\x1b\\",
    );
    assert!(server.render_retained_pane_surface_and_stream(&HashSet::from([pane_id])));
    assert_eq!(
        without_asset_payload(
            server.clients[&1]
                .render_state
                .last_pane_surface()
                .expect("baseline after full queue")
                .clone()
        ),
        without_asset_payload(baseline_before_queue)
    );
    server.clients.get_mut(&1).unwrap().writer = Some(original_writer);

    server.render_and_stream();
    let (_, recovered_message) = receive_message(&render_rx);
    let recovered = decode_surface_message(&mut decoder, recovered_message);
    assert!(!recovered.graphics.placements.is_empty());
    assert_eq!(recovered.graphics.assets.len(), 1);
    assert_eq!(recovered.graphics.assets[0].data, [0, 255, 0, 255]);
    shutdown_test_runtimes(&mut server);
}

fn drain_projection_and_surface(
    writer: &ClientWriter,
) -> (
    Option<Box<protocol::ClientShellSnapshot>>,
    Vec<ServerMessage>,
) {
    let mut snapshot: Option<Box<protocol::ClientShellSnapshot>> = None;
    let mut view: Option<protocol::endpoint::EndpointAgentViewProjection> = None;
    let mut completions: Option<protocol::endpoint::EndpointAgentCompletions> = None;
    let mut surfaces = Vec::new();
    for bytes in writer.test_drain() {
        let mut cursor = std::io::Cursor::new(bytes);
        while cursor.position() < cursor.get_ref().len() as u64 {
            let message: ServerMessage =
                protocol::read_message(&mut cursor, protocol::MAX_GRAPHICS_FRAME_SIZE).unwrap();
            match message {
                ServerMessage::EndpointControl { kind, data }
                    if kind == protocol::endpoint::ENDPOINT_SNAPSHOT_KIND =>
                {
                    snapshot = Some(Box::new(serde_json::from_str(&data).unwrap()));
                }
                ServerMessage::EndpointControl { kind, data }
                    if kind == protocol::endpoint::AGENT_VIEW_PROJECTION_KIND =>
                {
                    view = Some(serde_json::from_str(&data).unwrap());
                }
                ServerMessage::EndpointControl { kind, data }
                    if kind == protocol::endpoint::AGENT_COMPLETIONS_KIND =>
                {
                    completions = Some(serde_json::from_str(&data).unwrap());
                }
                message => surfaces.push(message),
            }
        }
    }
    if let Some(snapshot) = &snapshot {
        let view = view.unwrap();
        let completions = completions.unwrap();
        assert_eq!(view.boot_id, snapshot.boot_id);
        assert_eq!(view.revision, snapshot.revision);
        assert_eq!(completions.boot_id, snapshot.boot_id);
        assert_eq!(completions.revision, snapshot.revision);
        let label = view
            .view
            .map(serde_json::from_value::<crate::api::schema::AgentViewSetParams>)
            .transpose()
            .unwrap()
            .and_then(|view| view.label);
        assert_eq!(label, snapshot.agent_view_label);
    }
    (snapshot, surfaces)
}

#[tokio::test]
async fn replaced_pending_surface_and_patch_resync_before_delta_scroll_chain() {
    assert_replaced_pending_resync(false).await;
}

#[tokio::test]
async fn replaced_pending_surface_and_patch_resync_before_reuse_scroll_chain() {
    assert_replaced_pending_resync(true).await;
}

async fn assert_replaced_pending_resync(reuse_only: bool) {
    let (mut server, _control, _render, pane) = retained_test_server_with_control(b"initial text");
    let writer = ClientWriter::test_paused();
    {
        let client = server.clients.get_mut(&1).unwrap();
        client.writer = Some(writer.clone());
        client.render_state.enable_surface_reuse(true);
        client.render_state.enable_surface_delta(!reuse_only);
        client.render_state.enable_surface_scroll(true);
    }
    // Commit a frame without letting the consumer claim it, then change both
    // projection geometry and text. No decoder may require that discarded base.
    server.render_and_stream();
    write_shared_test_pane(&mut server, pane, b"\rLATEST first replacement");
    server.app.state.workspaces[0].custom_name = Some("latest projection".into());
    server.handle_server_event(ServerEvent::ClientShellResize {
        client_id: 1,
        surface_cols: 64,
        surface_rows: 18,
        cell_width_px: 0,
        cell_height_px: 0,
        pixel_mouse: false,
    });
    server.render_and_stream();
    let (snapshot, mut messages) = drain_projection_and_surface(&writer);
    assert_eq!(messages.len(), 1, "no obsolete presentation replay");
    assert!(
        matches!(&messages[0], ServerMessage::PaneSurface(_)),
        "full resync, not a delta against discarded work"
    );
    let mut decoder = protocol::surface_reuse::Decoder::new(!reuse_only, true);
    let surface = decode_surface_message(&mut decoder, messages.remove(0));
    let snapshot = snapshot.unwrap();
    assert_eq!(surface.boot_id, snapshot.boot_id);
    assert_eq!(surface.projection_revision, snapshot.revision);
    assert_eq!((surface.frame.width, surface.frame.height), (64, 18));
    assert!(frame_text(&surface.frame).contains("LATEST first replacement"));
    let mut receiver = crate::client::ClientShellState::new(
        crate::client::ClientShellConfig::from_config(&crate::config::Config::default()),
    );
    receiver.set_snapshot(snapshot);
    receiver.set_pane_surface(surface);
    assert!(frame_text(&receiver.compose(64, 20).unwrap()).contains("LATEST first replacement"));

    // An unclaimed row patch must roll back to the claimed base, not force a full grid.
    write_shared_test_pane(&mut server, pane, b"\rUNDLVD pending row patch");
    assert!(server.render_retained_pane_surface_and_stream(&HashSet::from([pane])));
    write_shared_test_pane(&mut server, pane, b"\rLATEST after lost patch ");
    server.app.state.workspaces[0].custom_name = Some("resynced projection".into());
    server.render_and_stream();
    let (snapshot, mut messages) = drain_projection_and_surface(&writer);
    assert_eq!(messages.len(), 1);
    if !reuse_only {
        assert!(
            matches!(&messages[0], ServerMessage::EndpointControl { kind, .. }
            if kind == protocol::surface_delta::MESSAGE_KIND)
        );
    }
    let surface = decode_surface_message(&mut decoder, messages.remove(0));
    let snapshot = snapshot.unwrap();
    assert_eq!(surface.projection_revision, snapshot.revision);
    receiver.set_snapshot(snapshot);
    receiver.set_pane_surface(surface);
    assert!(frame_text(&receiver.compose(64, 20).unwrap()).contains("LATEST after lost patch"));

    // Once a base has actually been claimed, metadata-only and row updates
    // can use negotiated reuse/delta/scroll again and pass the real receiver.
    server.app.state.workspaces[0].custom_name = Some("delta after resync".into());
    server.render_and_stream();
    let (snapshot, mut messages) = drain_projection_and_surface(&writer);
    assert_eq!(messages.len(), 1);
    let expected_kind = if reuse_only {
        protocol::surface_reuse::MESSAGE_KIND
    } else {
        protocol::surface_delta::MESSAGE_KIND
    };
    assert!(
        matches!(&messages[0], ServerMessage::EndpointControl { kind, .. } if kind == expected_kind)
    );
    let surface = decode_surface_message(&mut decoder, messages.remove(0));
    assert_eq!(
        surface.projection_revision,
        snapshot.as_ref().unwrap().revision
    );
    receiver.set_snapshot(snapshot.unwrap());
    receiver.set_pane_surface(surface);
    write_shared_test_pane(&mut server, pane, b"\rDELIVERED valid row patch");
    assert!(server.render_retained_pane_surface_and_stream(&HashSet::from([pane])));
    let (_, mut messages) = drain_projection_and_surface(&writer);
    assert_eq!(messages.len(), 1);
    let ServerMessage::PaneSurfacePatch(patch) = decoder.decode(messages.remove(0)).unwrap() else {
        panic!("decoded retained patch");
    };
    let _ = receiver.apply_pane_surface_patch(patch);
    assert!(frame_text(&receiver.compose(64, 20).unwrap()).contains("DELIVERED valid row patch"));
    let scrolling_lines = |range: std::ops::Range<usize>| {
        range
            .map(|n| {
                format!(
                    "build step {n:>3}: compiled module_{n} in {}ms\r\n",
                    n * 7 % 97
                )
            })
            .collect::<String>()
            .into_bytes()
    };
    write_shared_test_pane(&mut server, pane, &scrolling_lines(0..40));
    server
        .clients
        .get_mut(&1)
        .unwrap()
        .render_state
        .request_recompute();
    server.render_and_stream();
    let (_, mut messages) = drain_projection_and_surface(&writer);
    assert_eq!(messages.len(), 1);
    let mut scrolling_surface = decode_surface_message(&mut decoder, messages.remove(0));
    receiver.set_pane_surface(scrolling_surface.clone());
    write_shared_test_pane(&mut server, pane, &scrolling_lines(40..42));
    assert!(server.render_retained_pane_surface_and_stream(&HashSet::from([pane])));
    let (_, mut messages) = drain_projection_and_surface(&writer);
    assert_eq!(messages.len(), 1);
    // Scroll compression is opportunistic: a valid retained update may remain
    // a plain row patch when no profitable shift is detected. Check the decoded
    // base and complete consumer result, not that optional wire representation.
    let ServerMessage::PaneSurfacePatch(patch) = decoder.decode(messages.remove(0)).unwrap() else {
        panic!("expanded scroll patch after coherent resync");
    };
    assert_eq!(patch.boot_id, scrolling_surface.boot_id);
    assert_eq!(
        patch.projection_revision,
        scrolling_surface.projection_revision
    );
    assert_eq!(
        patch.base_surface_revision,
        scrolling_surface.surface_revision
    );
    assert_eq!(
        patch.surface_revision,
        scrolling_surface.surface_revision + 1
    );
    crate::server::render_stream::apply_pane_surface_patch(&mut scrolling_surface, &patch);
    let committed = server.clients[&1].render_state.last_pane_surface().unwrap();
    assert_eq!(
        &scrolling_surface, committed,
        "decoded scrolling cells/layout must exactly match sender commit"
    );
    let _ = receiver.apply_pane_surface_patch(patch);
    let painted = receiver.compose(64, 20).unwrap();
    assert!(frame_text(&painted).contains("module_41"));
    let mut full_receiver = crate::client::ClientShellState::new(
        crate::client::ClientShellConfig::from_config(&crate::config::Config::default()),
    );
    full_receiver.set_snapshot(Box::new(
        server.clients[&1]
            .shell_snapshot
            .as_deref()
            .unwrap()
            .clone(),
    ));
    full_receiver.set_pane_surface(committed.clone());
    let full_painted = full_receiver.compose(64, 20).unwrap();
    assert_eq!(
        painted.cells, full_painted.cells,
        "real patched consumer must paint the exact full-frame result"
    );
    assert_eq!(painted.cursor, full_painted.cursor);
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn metadata_churn_keeps_only_latest_coherent_paint_at_each_consumer_turn() {
    let (mut server, _control, _render, pane) = retained_test_server_with_control(b"initial");
    let writer = ClientWriter::test_paused();
    server.clients.get_mut(&1).unwrap().writer = Some(writer.clone());
    for number in 1..=200 {
        server.app.state.workspaces[0].custom_name = Some(format!("revision {number}"));
        server.app.state.agent_view_override =
            (number % 40 != 0).then(|| crate::api::schema::AgentViewSetParams {
                source: "churn-test".into(),
                label: Some(format!("view {number}")),
                filter: None,
                sort: Vec::new(),
            });
        let marker = format!("LATEST_INPUT_{number:03}");
        write_shared_test_pane(&mut server, pane, format!("\r{marker}\x1b[K").as_bytes());
        server.render_and_stream();
        if number % 20 == 0 {
            let (snapshot, mut messages) = drain_projection_and_surface(&writer);
            assert_eq!(
                messages.len(),
                1,
                "consumer must not replay intermediate metadata/surfaces"
            );
            let ServerMessage::PaneSurface(surface) = messages.remove(0) else {
                panic!("replacement requires a complete fresh surface");
            };
            let snapshot = snapshot.unwrap();
            assert_eq!(
                snapshot.revision,
                server.clients[&1].shell_projection_revision
            );
            assert_eq!(surface.projection_revision, snapshot.revision);
            assert_eq!(surface.boot_id, snapshot.boot_id);
            assert!(frame_text(&surface.frame).contains(&marker));
            if number % 40 == 0 {
                assert!(
                    snapshot.agent_view_label.is_none(),
                    "latest clear must survive replacement"
                );
            }
        }
    }
    shutdown_test_runtimes(&mut server);
}

#[tokio::test]
async fn negotiated_snapshot_and_surface_deltas_survive_replacement_in_real_receiver() {
    let (mut server, _control, _render, pane) =
        retained_test_server_with_control(b"initial combined");
    let writer = ClientWriter::test_paused_with_snapshot_delta();
    {
        let client = server.clients.get_mut(&1).unwrap();
        client.writer = Some(writer.clone());
        client.shell_snapshot = None;
        client.render_state.enable_surface_delta(true);
        client.render_state.enable_surface_reuse(true);
        client.render_state.request_repaint();
    }
    let mut receiver = crate::client::ClientShellState::new(
        crate::client::ClientShellConfig::from_config(&crate::config::Config::default()),
    );
    let mut snapshot: Option<protocol::ClientShellSnapshot> = None;
    let mut decoder = protocol::surface_reuse::Decoder::new(true, true);
    let mut snapshot_decoder = protocol::snapshot_delta::SnapshotDecoder::new(true);
    let mut snapshot_deltas = 0;
    let mut surface_deltas = 0;
    for turn in 0..=100 {
        let marker = format!("VISIBLE_COMBINED_{turn:03}");
        server.app.state.workspaces[0].custom_name = Some(format!("combined {turn}"));
        write_shared_test_pane(&mut server, pane, format!("\r{marker}\x1b[K").as_bytes());
        server.render_and_stream();
        if turn % 20 != 0 {
            continue;
        }
        let mut view_revision = None;
        let mut completion_revision = None;
        let mut surface_count = 0;
        for frame in writer.test_drain() {
            let message = read_server_message(frame);
            match message {
                ServerMessage::EndpointControl { kind, data }
                    if kind == protocol::endpoint::AGENT_VIEW_PROJECTION_KIND =>
                {
                    let view: protocol::endpoint::EndpointAgentViewProjection =
                        serde_json::from_str(&data).unwrap();
                    assert!(view.view.is_none());
                    view_revision = Some((view.boot_id, view.revision));
                }
                ServerMessage::EndpointControl { kind, data }
                    if kind == protocol::endpoint::AGENT_COMPLETIONS_KIND =>
                {
                    let completions: protocol::endpoint::EndpointAgentCompletions =
                        serde_json::from_str(&data).unwrap();
                    completion_revision = Some((completions.boot_id, completions.revision));
                }
                ServerMessage::EndpointControl { kind, data }
                    if kind == protocol::endpoint::ENDPOINT_SNAPSHOT_KIND
                        || kind == protocol::endpoint::SNAPSHOT_DELTA_CODEC_V1 =>
                {
                    if kind == protocol::endpoint::SNAPSHOT_DELTA_CODEC_V1 {
                        snapshot_deltas += 1;
                    } else {
                        assert_eq!(turn, 0, "steady churn must not resend the full inventory");
                    }
                    let next = *snapshot_decoder.decode(&kind, &data).unwrap().unwrap();
                    assert_eq!(view_revision, Some((next.boot_id.clone(), next.revision)));
                    assert_eq!(completion_revision, view_revision);
                    receiver.set_snapshot(Box::new(next.clone()));
                    snapshot = Some(next);
                }
                surface => {
                    if matches!(&surface, ServerMessage::EndpointControl { kind, .. }
                        if kind == protocol::surface_delta::MESSAGE_KIND)
                    {
                        surface_deltas += 1;
                    } else {
                        assert_eq!(turn, 0, "steady churn must not resend the full grid");
                    }
                    let surface = decode_surface_message(&mut decoder, surface);
                    let snapshot = snapshot.as_ref().unwrap();
                    assert_eq!(surface.boot_id, snapshot.boot_id);
                    assert_eq!(surface.projection_revision, snapshot.revision);
                    receiver.set_pane_surface(surface);
                    surface_count += 1;
                }
            }
        }
        assert_eq!(
            surface_count, 1,
            "only the newest pending presentation is visible"
        );
        assert!(frame_text(&receiver.compose(80, 25).unwrap()).contains(&marker));
    }
    assert_eq!(snapshot_deltas, 5);
    assert_eq!(surface_deltas, 5);

    // A new writer/connection cannot inherit the old connection's claimed bases.
    let reconnect = ClientWriter::test_paused_with_snapshot_delta();
    let client = server.clients.get_mut(&1).unwrap();
    client.writer = Some(reconnect.clone());
    client.shell_snapshot = None;
    client.render_state.reset_baseline();
    server.render_and_stream();
    let messages = reconnect
        .test_drain()
        .into_iter()
        .map(read_server_message)
        .collect::<Vec<_>>();
    assert!(messages.iter().any(|message| matches!(message,
        ServerMessage::EndpointControl { kind, .. } if kind == protocol::endpoint::ENDPOINT_SNAPSHOT_KIND)));
    assert!(messages
        .iter()
        .any(|message| matches!(message, ServerMessage::PaneSurface(_))));
    assert!(!messages.iter().any(|message| matches!(message,
        ServerMessage::EndpointControl { kind, .. } if kind == protocol::endpoint::SNAPSHOT_DELTA_CODEC_V1)));
    shutdown_test_runtimes(&mut server);
}
