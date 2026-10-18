use super::*;
use crate::protocol::endpoint::{
    terminal_session_geometry_valid, TerminalSessionMode, TerminalSessionRequest,
    TerminalSessionState, TERMINAL_SESSION_STATE_KIND,
};
use crate::server::clients::ManagedTerminalSession;

impl HeadlessServer {
    pub(super) fn set_managed_terminal_mode(&mut self, client_id: u64, data: String) -> bool {
        let request = if data.len() <= 4096 {
            serde_json::from_str::<TerminalSessionRequest>(&data).ok()
        } else {
            None
        };
        let Some(request) = request else {
            self.send_to_client(client_id, ServerMessage::ServerShutdown {
                reason: Some("invalid terminal.session.set_mode request".into()),
            });
            self.remove_client_and_resize_if_needed(client_id);
            return false;
        };
        let Some(client) = self.clients.get(&client_id) else { return false };
        let bound = client.managed_terminal.as_ref();
        let terminal_id = bound.map(|session| session.terminal_id.clone()).or_else(|| {
            self.resolve_terminal_target_id_string(&request.target)
        });
        let error = if !terminal_session_geometry_valid(request.cols, request.rows)
            || client.cell_size.width_px > 4096 || client.cell_size.height_px > 4096
        {
            Some("terminal session geometry exceeds the safe non-empty limit")
        } else if let Some(bound) = bound {
            if request.target != bound.target {
                Some("managed terminal session cannot change its bound target")
            } else if request.request_id <= bound.last_request_id {
                Some("terminal session request_id must increase")
            } else {
                None
            }
        } else if !matches!(client.mode, ClientConnectionMode::TerminalPending)
            || !matches!(client.render_state, crate::server::render_stream::ClientRenderState::TerminalAnsi { .. })
        {
            Some("managed terminal session requires a pending TerminalAnsi connection")
        } else if request.request_id != 0 || request.mode != TerminalSessionMode::Observe {
            Some("managed terminal session must start in observe with request_id 0")
        } else {
            None
        };
        let error = error.or_else(|| {
            if request.mode == TerminalSessionMode::Unknown {
                return Some("unsupported terminal session mode");
            }
            let Some(terminal_id) = terminal_id.as_deref() else {
                return Some("terminal session target not found");
            };
            if self.runtime_for_terminal_id_string(terminal_id).is_none() {
                return Some("bound terminal session no longer exists");
            }
            if request.mode == TerminalSessionMode::Control {
                if self.handoff_in_progress {
                    return Some("terminal session control unavailable during handoff");
                }
                if self.terminal_attach_owners.get(terminal_id).is_some_and(|owner| *owner != client_id) {
                    return Some("terminal already has an attached client; managed sessions never take over");
                }
                if self.pending_alt_screen_reads.iter().any(|pending| pending.terminal_id.as_str() == terminal_id) {
                    return Some("terminal has a read in progress; retry control later");
                }
            }
            None
        });

        if error.is_none() {
            let terminal_id = terminal_id.as_ref().expect("validated terminal");
            let real_terminal_id = self.terminal_id_by_string(terminal_id).expect("validated runtime");
            let stamp = self.allocate_activity_stamp();
            let client = self.clients.get_mut(&client_id).expect("connected client");
            if let Some(session) = client.managed_terminal.as_mut() {
                session.last_request_id = request.request_id;
            } else {
                client.managed_terminal = Some(ManagedTerminalSession {
                    target: request.target.clone(),
                    terminal_id: terminal_id.clone(),
                    last_request_id: request.request_id,
                });
            }
            client.last_activity = stamp;
            match request.mode {
                TerminalSessionMode::Control => {
                    client.mode = ClientConnectionMode::TerminalAttach { terminal_id: terminal_id.clone() };
                    client.terminal_size = (request.cols, request.rows);
                    let cell_size = client.cell_size;
                    self.terminal_attach_owners.insert(terminal_id.clone(), client_id);
                    self.app.state.direct_attach_resize_locks.insert(real_terminal_id.clone());
                    self.app.start_pending_agent_resume_for_terminal(
                        &real_terminal_id, request.rows, request.cols, true,
                    );
                    if let Some(runtime) = self.app.terminal_runtimes.get(&real_terminal_id) {
                        runtime.resize(request.rows, request.cols, cell_size.width_px, cell_size.height_px);
                    }
                }
                TerminalSessionMode::Observe => {
                    client.mode = ClientConnectionMode::TerminalObserve { terminal_id: terminal_id.clone() };
                    // Releasing a lease must not restore shell/client geometry: observation
                    // renders the actual remote grid and never resizes the source.
                    if self.terminal_attach_owners.get(terminal_id) == Some(&client_id) {
                        self.terminal_attach_owners.remove(terminal_id);
                        self.app.state.direct_attach_resize_locks.remove(&real_terminal_id);
                    }
                }
                TerminalSessionMode::Unknown => unreachable!(),
            }
            if self.foreground_client_id == Some(client_id) {
                self.promote_latest_remaining_client();
            }
        } else if let Some(session) = self.clients.get_mut(&client_id).and_then(|client| client.managed_terminal.as_mut()) {
            // A denial is still a consumed request; replay must not later acquire a lease.
            session.last_request_id = session.last_request_id.max(request.request_id);
        }

        self.reply_managed_terminal_state(client_id, &request, error.map(str::to_owned));
        error.is_none()
    }

    fn reply_managed_terminal_state(
        &mut self,
        client_id: u64,
        request: &TerminalSessionRequest,
        error: Option<String>,
    ) {
        let Some(client) = self.clients.get(&client_id) else { return };
        let mode = if matches!(client.mode, ClientConnectionMode::TerminalAttach { .. }) {
            TerminalSessionMode::Control
        } else {
            TerminalSessionMode::Observe
        };
        let target = client.managed_terminal.as_ref().map_or(&request.target, |bound| &bound.target).clone();
        let terminal_id = client.managed_terminal.as_ref().map(|bound| bound.terminal_id.clone());
        let dimensions = terminal_id.as_deref()
            .and_then(|id| self.runtime_for_terminal_id_string(id))
            .and_then(|runtime| runtime.terminal_dimensions());
        let (cols, rows) = dimensions.unwrap_or(client.terminal_size);
        let mut repaint_after_epoch = false;
        let frame = terminal_id.as_deref()
            .and_then(|id| self.runtime_for_terminal_id_string(id))
            .and_then(|runtime| {
                let (synchronized, epoch) = runtime.synchronized_output_state();
                if synchronized {
                    return None;
                }
                let area = Rect::new(0, 0, cols, rows);
                let (buffer, cursor) = crate::server::render_stream::render_terminal_virtual(runtime, area);
                let hyperlinks = runtime.visible_hyperlinks(area);
                let (synchronized, after_epoch) = runtime.synchronized_output_state();
                if synchronized || after_epoch != epoch {
                    repaint_after_epoch = !synchronized;
                    return None;
                }
                Some(FrameData::from_ratatui_buffer_with_hyperlinks(&buffer, cursor, &hyperlinks))
            });
        if repaint_after_epoch {
            // A whole synchronized update can race the snapshot. Its commit may
            // already have been drained, so explicitly retry a coherent full frame.
            self.app.render_dirty.request_generic();
        }
        let state = TerminalSessionState { target, mode, cols, rows, request_id: request.request_id, error };
        let serialized = (|| -> Result<Vec<u8>, String> {
            let message = ServerMessage::EndpointControl {
                kind: TERMINAL_SESSION_STATE_KIND.into(),
                data: serde_json::to_string(&state).map_err(|error| error.to_string())?,
            };
            let mut serialized = Self::frame_server_message(&message).map_err(|error| error.to_string())?;
            let client = self.clients.get_mut(&client_id).expect("connected client");
            client.terminal_size = (cols, rows);
            client.render_state.reset_baseline();
            client.clear_deferred_render();
            let prepared = frame.and_then(|frame| client.render_state.prepare_frame(frame));
            if let Some(prepared) = prepared {
                serialized.extend(Self::frame_server_message(prepared.message()).map_err(|error| error.to_string())?);
                client.render_state.commit_sent_frame(prepared);
            }
            // During synchronized output, the reset baseline guarantees that the
            // first frame after the reply is full, once the PTY commits its update.
            Ok(serialized)
        })();
        let sent = match serialized {
            Ok(bytes) => self.clients.get(&client_id).and_then(|client| client.writer.as_ref())
                .is_some_and(|writer| writer.send_terminal_transition(bytes).is_ok()),
            Err(error) => {
                warn!(client_id, %error, "failed to encode terminal session state");
                false
            }
        };
        if !sent {
            self.remove_client_and_resize_if_needed(client_id);
        }
    }
}
