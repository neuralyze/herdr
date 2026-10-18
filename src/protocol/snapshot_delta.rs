//! Optional sparse replacement snapshots. Full V1 snapshots always seed/reset the base.
//!
//! Delta V1 has a closed patch vocabulary: adding snapshot fields requires a new
//! negotiated delta codec (or sending full V1 snapshots), not silently extending it.
//! Row values retain the full snapshot's additive-field compatibility rules.

use serde::{Deserialize, Serialize};

use super::endpoint::{ENDPOINT_SNAPSHOT_KIND, SNAPSHOT_DELTA_CODEC_V1};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDelta {
    pub boot_id: String,
    pub base_revision: u64,
    pub revision: u64,
    pub changes: Vec<SnapshotChange>,
}

// This must remain a non-transparent newtype, not a deserialize_with hook on
// Option<T>: serde's adjacent-tag missing-content path otherwise synthesizes
// None without calling that hook. The wrapper requires an actual value token;
// its inner Option still accepts explicit null, without changing wire shape.
#[derive(Deserialize)]
struct SnapshotValue<T>(T);

/// Index replacements followed by truncation. Appended rows must all be present;
/// indices are strictly increasing, so duplicates cannot ambiguously overwrite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRows<T> {
    pub length: usize,
    pub rows: Vec<SnapshotRow<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRow<T> {
    pub index: usize,
    pub value: T,
}

impl<T: Clone + PartialEq> SnapshotRows<T> {
    fn between(current: &[T], base: &[T]) -> Self {
        Self {
            length: current.len(),
            rows: current
                .iter()
                .enumerate()
                .filter(|(index, value)| base.get(*index) != Some(*value))
                .map(|(index, value)| SnapshotRow {
                    index,
                    value: value.clone(),
                })
                .collect(),
        }
    }

    fn validate(&self, base_len: usize) -> Result<(), String> {
        let mut previous = None;
        let mut next_append = base_len;
        for row in &self.rows {
            if row.index >= self.length || previous.is_some_and(|index| index >= row.index) {
                return Err("invalid snapshot delta row index".into());
            }
            if row.index >= base_len {
                if row.index != next_append {
                    return Err("snapshot delta omits appended row".into());
                }
                next_append += 1;
            }
            previous = Some(row.index);
        }
        if self.length > next_append {
            return Err("snapshot delta omits appended row".into());
        }
        Ok(())
    }

    fn install(self, base: &mut Vec<T>) {
        for row in self.rows {
            if row.index == base.len() {
                base.push(row.value);
            } else {
                base[row.index] = row.value;
            }
        }
        base.truncate(self.length);
    }
}

// Keep the typed wire vocabulary, comparisons and installation together. Only
// changed fields/rows are cloned; unchanged inventories/config are not encoded.
macro_rules! snapshot_changes {
    (fields { $( $variant:ident => $field:ident : $ty:ty, )* }
     rows { $( $rows_variant:ident => $rows_field:ident : $row_ty:ty, )* }) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
        #[serde(tag = "field", content = "value", rename_all = "snake_case", deny_unknown_fields)]
        pub enum SnapshotChange {
            $( $variant($ty), )*
            $( $rows_variant(SnapshotRows<$row_ty>), )*
        }

        // Separate typed wire decoding makes presence mandatory even for
        // nullable variants, while preserving serde's order-independent tags,
        // duplicate/unknown-key rejection and typed collection decoding.
        #[derive(Deserialize)]
        #[serde(tag = "field", content = "value", rename_all = "snake_case", deny_unknown_fields)]
        enum SnapshotChangeWire {
            $( $variant(SnapshotValue<$ty>), )*
            $( $rows_variant(SnapshotValue<SnapshotRows<$row_ty>>), )*
        }

        impl<'de> Deserialize<'de> for SnapshotChange {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Ok(match SnapshotChangeWire::deserialize(deserializer)? {
                    $( SnapshotChangeWire::$variant(SnapshotValue(value)) => Self::$variant(value), )*
                    $( SnapshotChangeWire::$rows_variant(SnapshotValue(value)) => Self::$rows_variant(value), )*
                })
            }
        }

        impl SnapshotDelta {
            fn between(current: &ClientShellSnapshot, base: &ClientShellSnapshot) -> Self {
                // Adding a full snapshot field requires considering the closed
                // delta vocabulary, rather than silently losing that field.
                let ClientShellSnapshot {
                    boot_id: _, revision: _, $( $field: _, )* $( $rows_field: _, )*
                } = current;
                let mut changes = Vec::new();
                $( if current.$field != base.$field {
                    changes.push(SnapshotChange::$variant(current.$field.clone()));
                } )*
                $(
                    let rows = SnapshotRows::between(&current.$rows_field, &base.$rows_field);
                    if rows.length != base.$rows_field.len() || !rows.rows.is_empty() {
                        changes.push(SnapshotChange::$rows_variant(rows));
                    }
                )*
                Self {
                    boot_id: current.boot_id.clone(),
                    base_revision: base.revision,
                    revision: current.revision,
                    changes,
                }
            }
        }

        impl SnapshotChange {
            fn validate(&self, base: &ClientShellSnapshot) -> Result<(), String> {
                match self {
                    $( Self::$variant(_) => Ok(()), )*
                    $( Self::$rows_variant(rows) => rows.validate(base.$rows_field.len()), )*
                }
            }

            fn install(self, target: &mut ClientShellSnapshot) {
                match self {
                    $( Self::$variant(value) => target.$field = value, )*
                    $( Self::$rows_variant(rows) => rows.install(&mut target.$rows_field), )*
                }
            }
        }
    }
}

snapshot_changes! {
    fields {
        ConfigDiagnostic => config_diagnostic: Option<String>,
        ProductAnnouncement => product_announcement: Option<ClientShellProductAnnouncement>,
        UpdateAvailable => update_available: Option<String>,
        UpdateInstallCommand => update_install_command: String,
        ServerKeybindingsToml => server_keybindings_toml: Option<String>,
        LatestReleaseNotesAvailable => latest_release_notes_available: bool,
        IntegrationUpdatesAvailable => integration_updates_available: bool,
        WorktreeDirectory => worktree_directory: String,
        ReleaseNotes => release_notes: Option<ClientShellReleaseNotes>,
        FocusedWorkspaceId => focused_workspace_id: Option<String>,
        FocusedTabId => focused_tab_id: Option<String>,
        FocusedPaneId => focused_pane_id: Option<String>,
        TabBarRight => tab_bar_right: Vec<ClientShellTabStatusSegment>,
        TabBarRightSeparator => tab_bar_right_separator: String,
        AgentViewLabel => agent_view_label: Option<String>,
        AgentOrder => agent_order: Vec<String>,
    }
    rows {
        Workspaces => workspaces: ClientShellWorkspace,
        Tabs => tabs: ClientShellTab,
        Panes => panes: ClientShellPane,
        Agents => agents: ClientShellAgent,
        Commands => commands: ClientShellCommand,
    }
}

/// Reconstruct atomically: failure never changes the caller's retained base.
#[cfg(test)]
pub fn apply_snapshot_delta(
    base: &ClientShellSnapshot,
    delta: SnapshotDelta,
) -> Result<ClientShellSnapshot, String> {
    delta.validate(base)?;
    let mut next = base.clone();
    delta.install(&mut next);
    Ok(next)
}

impl SnapshotDelta {
    fn validate(&self, base: &ClientShellSnapshot) -> Result<(), String> {
        if self.boot_id != base.boot_id
            || self.base_revision != base.revision
            || self.revision <= base.revision
        {
            return Err("snapshot delta boot/revision does not match base".into());
        }
        for (index, change) in self.changes.iter().enumerate() {
            if self.changes[..index]
                .iter()
                .any(|previous| std::mem::discriminant(previous) == std::mem::discriminant(change))
            {
                return Err("duplicate snapshot delta field".into());
            }
            change.validate(base)?;
        }
        Ok(())
    }

    // Called only after all fallible validation, so installing needs no rollback
    // and the native decoder can reuse its single retained inventory allocation.
    fn install(self, target: &mut ClientShellSnapshot) {
        for change in self.changes {
            change.install(target);
        }
        target.revision = self.revision;
    }
}

// Retain a possible full fallback only while it can beat the delta. A sparse
// update usually crosses the limit in config/inventory serialization, so stop
// there rather than traversing the rest of the snapshot just to count bytes.
struct FullSnapshotCandidate {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl std::io::Write for FullSnapshotCandidate {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            self.exceeded = true;
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Encode against the last non-discardable full snapshot, never a pending one.
/// The selected delta codec still uses full V1 for bootstrap and resynchronization.
pub fn snapshot_message(
    current: &ClientShellSnapshot,
    base: Option<&ClientShellSnapshot>,
    allow_delta: bool,
) -> serde_json::Result<ServerMessage> {
    if let Some(base) = base.filter(|base| {
        allow_delta && base.boot_id == current.boot_id && current.revision > base.revision
    }) {
        let delta = serde_json::to_string(&SnapshotDelta::between(current, base))?;
        // Include kind bytes as well as payload bytes. Equality favors full V1.
        let mut full = FullSnapshotCandidate {
            bytes: Vec::new(),
            limit: delta.len() + SNAPSHOT_DELTA_CODEC_V1.len() - ENDPOINT_SNAPSHOT_KIND.len(),
            exceeded: false,
        };
        let encoded = serde_json::to_writer(&mut full, current);
        if full.exceeded {
            return Ok(ServerMessage::EndpointControl {
                kind: SNAPSHOT_DELTA_CODEC_V1.into(),
                data: delta,
            });
        }
        encoded?;
        return Ok(ServerMessage::EndpointControl {
            kind: ENDPOINT_SNAPSHOT_KIND.into(),
            data: String::from_utf8(full.bytes).expect("JSON output is UTF-8"),
        });
    }
    Ok(ServerMessage::EndpointControl {
        kind: ENDPOINT_SNAPSHOT_KIND.into(),
        data: serde_json::to_string(current)?,
    })
}

/// One negotiated connection owns one baseline. Reconnects create a new decoder.
#[derive(Default)]
pub(crate) struct SnapshotDecoder {
    allow_delta: bool,
    base: Option<ClientShellSnapshot>,
}

impl SnapshotDecoder {
    pub(crate) fn new(allow_delta: bool) -> Self {
        Self {
            allow_delta,
            base: None,
        }
    }

    pub(crate) fn decode(
        &mut self,
        kind: &str,
        data: &str,
    ) -> Result<Option<Box<ClientShellSnapshot>>, String> {
        let next: ClientShellSnapshot = if kind == ENDPOINT_SNAPSHOT_KIND {
            serde_json::from_str(data)
                .map_err(|error| format!("invalid endpoint snapshot: {error}"))?
        } else if kind == SNAPSHOT_DELTA_CODEC_V1 {
            if !self.allow_delta {
                return Err("unnegotiated endpoint snapshot delta".into());
            }
            let base = self
                .base
                .as_mut()
                .ok_or("endpoint snapshot delta has no base")?;
            let delta: SnapshotDelta = serde_json::from_str(data)
                .map_err(|error| format!("invalid endpoint snapshot delta: {error}"))?;
            delta.validate(base)?;
            delta.install(base);
            return Ok(Some(Box::new(base.clone())));
        } else {
            return Ok(None);
        };
        if self.allow_delta {
            self.base = Some(next.clone());
        }
        Ok(Some(Box::new(next)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> ClientShellSnapshot {
        serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/endpoint-snapshot-v1.json"
        )))
        .unwrap()
    }

    fn control(message: ServerMessage) -> (String, String) {
        match message {
            ServerMessage::EndpointControl { kind, data } => (kind, data),
            _ => panic!("snapshot control"),
        }
    }

    #[test]
    fn snapshot_delta_bootstrap_and_legacy_fallback() {
        let base = snapshot();
        let mut next = base.clone();
        next.revision += 1;
        next.focused_pane_id = None;
        for (current, previous, allow) in [
            (&next, None, true),
            (&next, Some(&base), false),
            (&base, Some(&next), true),
            (&base, Some(&base), true),
        ] {
            assert_eq!(
                control(snapshot_message(current, previous, allow).unwrap()).0,
                ENDPOINT_SNAPSHOT_KIND
            );
        }
        next.boot_id = "new boot".into();
        assert_eq!(
            control(snapshot_message(&next, Some(&base), true).unwrap()).0,
            ENDPOINT_SNAPSHOT_KIND
        );
    }

    #[test]
    fn snapshot_delta_omits_unchanged_config_and_inventory_and_clears_null() {
        let mut base = snapshot();
        base.server_keybindings_toml = Some("large configuration".repeat(1000));
        let mut next = base.clone();
        next.revision += 1;
        next.agent_view_label = None;
        next.focused_pane_id = Some("other pane".into());
        let (kind, data) = control(snapshot_message(&next, Some(&base), true).unwrap());
        assert_eq!(kind, SNAPSHOT_DELTA_CODEC_V1);
        assert!(!data.contains("server_keybindings_toml"));
        assert!(!data.contains("workspaces"));
        assert!(!data.contains("commands"));
        let value: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert!(
            value["changes"].as_array().unwrap().iter().any(|change| {
                change["field"] == "agent_view_label" && change["value"].is_null()
            })
        );
        assert_eq!(
            apply_snapshot_delta(&base, serde_json::from_str(&data).unwrap()).unwrap(),
            next
        );
    }

    #[test]
    fn snapshot_delta_rows_preserve_removals_reordering_appends_and_empty_values() {
        let mut base = snapshot();
        let mut second = base.panes[0].clone();
        second.pane_id = "second".into();
        let mut third = second.clone();
        third.pane_id = "third".into();
        base.panes.extend([second.clone(), third]);
        let mut next = base.clone();
        next.revision += 1;
        next.panes = vec![second, base.panes[0].clone()];
        next.panes[0].label = None;
        next.commands.clear();
        next.agent_order.clear();
        next.tab_bar_right.clear();
        next.tab_bar_right_separator.clear();
        assert_eq!(
            apply_snapshot_delta(&base, SnapshotDelta::between(&next, &base)).unwrap(),
            next
        );
        let mut appended = next.clone();
        appended.revision += 1;
        appended.panes.push(base.panes[2].clone());
        assert_eq!(
            apply_snapshot_delta(&next, SnapshotDelta::between(&appended, &next)).unwrap(),
            appended
        );
    }

    #[test]
    fn snapshot_delta_rejects_bad_base_boot_and_nonmonotonic_revision() {
        let base = snapshot();
        let mut next = base.clone();
        next.revision += 1;
        for invalid in 0..3 {
            let mut delta = SnapshotDelta::between(&next, &base);
            match invalid {
                0 => delta.base_revision += 1,
                1 => delta.boot_id.push('x'),
                _ => delta.revision = base.revision,
            }
            assert!(apply_snapshot_delta(&base, delta).is_err());
        }
        assert_eq!(base, snapshot());
    }

    #[test]
    fn snapshot_delta_nullable_fields_require_presence_but_accept_null_in_either_key_order() {
        let base = snapshot();
        let full = serde_json::to_string(&base).unwrap();
        for field in [
            "config_diagnostic",
            "product_announcement",
            "update_available",
            "server_keybindings_toml",
            "release_notes",
            "focused_workspace_id",
            "focused_tab_id",
            "focused_pane_id",
            "agent_view_label",
        ] {
            let envelope = |change: &str| {
                format!(
                    r#"{{"boot_id":"boot-v1","base_revision":7,"revision":8,"changes":[{change}]}}"#
                )
            };
            let missing = envelope(&format!(r#"{{"field":"{field}"}}"#));
            assert!(
                serde_json::from_str::<SnapshotDelta>(&missing).is_err(),
                "{field}: missing value accepted"
            );
            let mut expected = serde_json::to_value(&base).unwrap();
            expected[field] = serde_json::Value::Null;
            expected["revision"] = serde_json::json!(8);
            let expected: ClientShellSnapshot = serde_json::from_value(expected).unwrap();
            for change in [
                format!(r#"{{"field":"{field}","value":null}}"#),
                format!(r#"{{"value":null,"field":"{field}"}}"#),
            ] {
                let mut decoder = SnapshotDecoder::new(true);
                decoder.decode(ENDPOINT_SNAPSHOT_KIND, &full).unwrap();
                assert!(
                    decoder.decode(SNAPSHOT_DELTA_CODEC_V1, &missing).is_err(),
                    "{field}"
                );
                assert_eq!(
                    decoder.base.as_ref(),
                    Some(&base),
                    "{field}: malformed patch changed base"
                );
                let explicit_null = envelope(&change);
                let delta: SnapshotDelta = serde_json::from_str(&explicit_null).unwrap();
                assert_eq!(
                    apply_snapshot_delta(&base, delta).unwrap(),
                    expected,
                    "{field}"
                );
                assert_eq!(
                    *decoder
                        .decode(SNAPSHOT_DELTA_CODEC_V1, &explicit_null)
                        .unwrap()
                        .unwrap(),
                    expected,
                    "{field}"
                );
                assert_eq!(decoder.base.as_ref(), Some(&expected), "{field}");
                assert_eq!(base, snapshot(), "{field}: immutable apply base changed");
            }
        }
    }

    #[test]
    fn snapshot_delta_rejects_duplicate_fields_rows_missing_appends_and_unknown_patch_fields() {
        let base = snapshot();
        let make = |changes| SnapshotDelta {
            boot_id: base.boot_id.clone(),
            base_revision: base.revision,
            revision: base.revision + 1,
            changes,
        };
        assert!(
            apply_snapshot_delta(
                &base,
                make(vec![
                    SnapshotChange::AgentViewLabel(None),
                    SnapshotChange::AgentViewLabel(Some("x".into())),
                ])
            )
            .is_err()
        );
        for patch in [
            SnapshotRows {
                length: 1,
                rows: vec![
                    SnapshotRow {
                        index: 0,
                        value: base.panes[0].clone(),
                    },
                    SnapshotRow {
                        index: 0,
                        value: base.panes[0].clone(),
                    },
                ],
            },
            SnapshotRows {
                length: 3,
                rows: vec![SnapshotRow {
                    index: 2,
                    value: base.panes[0].clone(),
                }],
            },
            SnapshotRows {
                length: 0,
                rows: vec![SnapshotRow {
                    index: 0,
                    value: base.panes[0].clone(),
                }],
            },
            SnapshotRows {
                length: usize::MAX,
                rows: Vec::new(),
            },
        ] {
            assert!(apply_snapshot_delta(&base, make(vec![SnapshotChange::Panes(patch)])).is_err());
        }
        for changes in [
            r#"[{"field":"future_field","value":null}]"#,
            r#"[{"field":"agent_view_label"}]"#,
            r#"[{"field":"agent_order","value":[],"future":true}]"#,
            r#"[{"field":"panes","value":{"length":0,"rows":[],"future":true}}]"#,
            r#"[{"field":"panes","value":{"length":1,"rows":[{"index":0,"value":{},"future":true}]}}]"#,
            r#"[{"field":"agent_order","field":"agent_order","value":[]}]"#,
        ] {
            let json = format!(
                r#"{{"boot_id":"boot-v1","base_revision":7,"revision":8,"changes":{changes}}}"#
            );
            assert!(
                serde_json::from_str::<SnapshotDelta>(&json).is_err(),
                "{json}"
            );
        }
        assert_eq!(base, snapshot());
    }

    #[test]
    fn snapshot_delta_full_fallback_when_changes_are_larger() {
        let base = snapshot();
        let mut next = base.clone();
        next.revision += 1;
        next.commands = (0..1000)
            .map(|i| ClientShellCommand {
                command_id: i.to_string(),
                binding_label: String::new(),
                binding_labels: Vec::new(),
                action: ClientShellCommandAction::Shell,
                description: None,
            })
            .collect();
        let (kind, data) = control(snapshot_message(&next, Some(&base), true).unwrap());
        assert_eq!(kind, ENDPOINT_SNAPSHOT_KIND);
        assert_eq!(
            serde_json::from_str::<ClientShellSnapshot>(&data).unwrap(),
            next
        );
    }

    #[test]
    fn snapshot_delta_full_candidate_retains_exact_fallback_and_stops_at_limit() {
        let snapshot = snapshot();
        let encoded = serde_json::to_vec(&snapshot).unwrap();
        for limit in [encoded.len() - 1, encoded.len(), encoded.len() + 1] {
            let mut candidate = FullSnapshotCandidate {
                bytes: Vec::new(),
                limit,
                exceeded: false,
            };
            let result = serde_json::to_writer(&mut candidate, &snapshot);
            assert_eq!(candidate.exceeded, limit < encoded.len());
            assert_eq!(result.is_err(), candidate.exceeded);
            assert!(candidate.bytes.len() <= limit);
            if !candidate.exceeded {
                assert_eq!(candidate.bytes, encoded);
            }
        }
    }

    #[test]
    fn snapshot_delta_quote_and_backslash_heavy_changes_never_increase_actual_frame_size() {
        fn frame_len(message: &ServerMessage) -> usize {
            let mut bytes = Vec::new();
            super::super::write_message(&mut bytes, message).unwrap();
            bytes.len()
        }
        let base = snapshot();
        let mut saw_delta = false;
        let mut saw_full = false;
        for row_count in [0, 1, 32, 256] {
            for repeats in [0, 1, 16, 128] {
                let value = "\"quoted\\\\path\\\n".repeat(repeats);
                let mut next = base.clone();
                next.revision += 1;
                next.focused_pane_id = Some(value.clone());
                next.config_diagnostic = Some(value.clone());
                next.commands = (0..row_count)
                    .map(|index| ClientShellCommand {
                        command_id: format!("{index}:{value}"),
                        binding_label: value.clone(),
                        binding_labels: vec![value.clone()],
                        action: ClientShellCommandAction::Shell,
                        description: Some(value.clone()),
                    })
                    .collect();
                let full = snapshot_message(&next, None, false).unwrap();
                let selected = snapshot_message(&next, Some(&base), true).unwrap();
                let is_delta = matches!(&selected, ServerMessage::EndpointControl { kind, .. }
                    if kind == SNAPSHOT_DELTA_CODEC_V1);
                let selected_len = frame_len(&selected);
                let full_len = frame_len(&full);
                assert!(
                    selected_len <= full_len,
                    "rows={row_count}, repeats={repeats}, selected={selected_len}, full={full_len}"
                );
                if is_delta {
                    saw_delta = true;
                    assert!(
                        selected_len < full_len,
                        "delta must be strictly smaller on actual wire"
                    );
                } else {
                    saw_full = true;
                    assert_eq!(selected_len, full_len);
                }
            }
        }
        assert!(
            saw_delta && saw_full,
            "exercise both capped sparse and retained full paths"
        );
    }

    #[test]
    fn snapshot_delta_decoder_rejects_unnegotiated_and_recovers_only_from_full_seed() {
        let base = snapshot();
        let mut next = base.clone();
        next.revision += 1;
        next.agent_view_label = None;
        let (full_kind, full) = control(snapshot_message(&base, None, true).unwrap());
        let (kind, delta) = control(snapshot_message(&next, Some(&base), true).unwrap());
        assert_eq!(kind, SNAPSHOT_DELTA_CODEC_V1);
        let mut legacy = SnapshotDecoder::new(false);
        legacy.decode(&full_kind, &full).unwrap();
        assert!(legacy.decode(&kind, &delta).is_err());
        let mut decoder = SnapshotDecoder::new(true);
        assert!(decoder.decode(&kind, &delta).is_err());
        assert_eq!(*decoder.decode(&full_kind, &full).unwrap().unwrap(), base);
        let invalid = SnapshotDelta {
            boot_id: base.boot_id.clone(),
            base_revision: base.revision,
            revision: next.revision,
            changes: vec![
                SnapshotChange::ConfigDiagnostic(None),
                SnapshotChange::Panes(SnapshotRows {
                    length: 0,
                    rows: vec![SnapshotRow {
                        index: 0,
                        value: base.panes[0].clone(),
                    }],
                }),
            ],
        };
        assert!(
            decoder
                .decode(&kind, &serde_json::to_string(&invalid).unwrap())
                .is_err()
        );
        assert_eq!(
            decoder.base.as_ref(),
            Some(&base),
            "late invalid row must not install earlier scalar"
        );
        assert_eq!(*decoder.decode(&kind, &delta).unwrap().unwrap(), next);
        assert!(
            decoder.decode(&kind, &delta).is_err(),
            "duplicate revision rejected"
        );
        let mut reboot = next.clone();
        reboot.boot_id = "reboot".into();
        reboot.revision = 1;
        let (reset_kind, reset) = control(snapshot_message(&reboot, Some(&next), true).unwrap());
        assert_eq!(
            *decoder.decode(&reset_kind, &reset).unwrap().unwrap(),
            reboot
        );
        assert!(
            decoder.decode(&kind, &delta).is_err(),
            "old boot cannot follow new seed"
        );
        assert_eq!(decoder.base.as_ref(), Some(&reboot), "failure is atomic");
        let mut future = serde_json::to_value(&reboot).unwrap();
        future["future_root"] = serde_json::json!({"enabled":true});
        future["panes"][0]["future_row"] = serde_json::json!(true);
        assert_eq!(
            *decoder
                .decode(ENDPOINT_SNAPSHOT_KIND, &future.to_string())
                .unwrap()
                .unwrap(),
            reboot
        );
    }
}
