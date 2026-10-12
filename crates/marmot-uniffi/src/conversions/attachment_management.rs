//! Account/scope-bound native management handles. Never serialize or log these objects.
use super::{AttachmentEntryFfi, AttachmentTransferStatusFfi, HistoryNoticeFfi};
use marmot_app as app;
use std::sync::Arc;
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum AttachmentJobViewFfi {
    #[default]
    All,
    Active,
    NeedsAttention,
}
#[derive(Clone, Copy, Debug, Default, uniffi::Enum)]
pub enum AttachmentJobOriginFfi {
    #[default]
    Any,
    Automatic,
    Explicit,
}
#[derive(Clone, uniffi::Record)]
pub struct AttachmentJobQueryFfi {
    pub group_id_hex: String,
    pub view: AttachmentJobViewFfi,
    pub origin: AttachmentJobOriginFfi,
}
impl From<AttachmentJobQueryFfi> for app::AttachmentJobQuery {
    fn from(q: AttachmentJobQueryFfi) -> Self {
        Self {
            group_id_hex: q.group_id_hex,
            view: match q.view {
                AttachmentJobViewFfi::All => app::AttachmentJobView::All,
                AttachmentJobViewFfi::Active => app::AttachmentJobView::Active,
                AttachmentJobViewFfi::NeedsAttention => app::AttachmentJobView::NeedsAttention,
            },
            origin: match q.origin {
                AttachmentJobOriginFfi::Any => app::AttachmentJobOrigin::Any,
                AttachmentJobOriginFfi::Automatic => app::AttachmentJobOrigin::Automatic,
                AttachmentJobOriginFfi::Explicit => app::AttachmentJobOrigin::Explicit,
            },
        }
    }
}
macro_rules! opaque_object {
    ($name:ident,$ty:ty) => {
        #[derive(Debug, uniffi::Object)]
        pub struct $name {
            pub(crate) inner: $ty,
        }
    };
}
opaque_object!(AttachmentJobCursor, app::AttachmentJobCursor);
opaque_object!(
    AttachmentManagementVersion,
    app::AttachmentManagementVersion
);
#[uniffi::export]
impl AttachmentManagementVersion {
    /// Compare complete replacement generations; false requires replacing the snapshot.
    pub fn same_as(&self, previous: Arc<AttachmentManagementVersion>) -> bool {
        self.inner.same_as(&previous.inner)
    }
}
#[derive(Clone, uniffi::Record)]
pub struct ManagedAttachmentEntryFfi {
    pub group_id_hex: String,
    pub entry: AttachmentEntryFfi,
    pub explicit: bool,
    pub status: AttachmentTransferStatusFfi,
}
#[derive(Clone, uniffi::Record)]
pub struct ManagedAttachmentPageFfi {
    pub entries: Vec<ManagedAttachmentEntryFfi>,
    pub next_cursor: Option<Arc<AttachmentJobCursor>>,
    pub next_expiry: Option<u64>,
    pub observed_at: u64,
}
impl From<app::ManagedAttachmentPage> for ManagedAttachmentPageFfi {
    fn from(p: app::ManagedAttachmentPage) -> Self {
        Self {
            entries: p
                .entries
                .into_iter()
                .map(|e| ManagedAttachmentEntryFfi {
                    group_id_hex: e.group_id_hex,
                    entry: e.entry.into(),
                    explicit: e.explicit,
                    status: Some(e.status).into(),
                })
                .collect(),
            next_cursor: p
                .next_cursor
                .map(|inner| Arc::new(AttachmentJobCursor { inner })),
            next_expiry: p.next_expiry,
            observed_at: p.observed_at,
        }
    }
}
#[derive(Clone, Debug, uniffi::Record)]
pub struct AttachmentJobCountsFfi {
    pub active: u32,
    pub needs_attention: u32,
    pub paused: u32,
    pub cancelled: u32,
    pub policy_blocked: u32,
    pub other: u32,
    pub complete: bool,
}
impl From<app::AttachmentJobCounts> for AttachmentJobCountsFfi {
    fn from(c: app::AttachmentJobCounts) -> Self {
        Self {
            active: c.active,
            needs_attention: c.needs_attention,
            paused: c.paused,
            cancelled: c.cancelled,
            policy_blocked: c.policy_blocked,
            other: c.other,
            complete: c.complete,
        }
    }
}
#[derive(Clone, uniffi::Record)]
pub struct AttachmentManagementSnapshotFfi {
    pub available: bool,
    pub automatic_recovery_failed: Option<bool>,
    pub notices: Vec<HistoryNoticeFfi>,
    pub notices_complete: bool,
    pub counts: AttachmentJobCountsFfi,
    pub page: ManagedAttachmentPageFfi,
    pub version: Arc<AttachmentManagementVersion>,
    pub redacted_diagnostics: String,
}
impl From<app::AttachmentManagementSnapshot> for AttachmentManagementSnapshotFfi {
    fn from(s: app::AttachmentManagementSnapshot) -> Self {
        let diagnostics = s.redacted_diagnostics();
        Self {
            available: s.available,
            automatic_recovery_failed: s.automatic_recovery_failed,
            notices: s.notices.into_iter().map(Into::into).collect(),
            notices_complete: s.notices_complete,
            counts: s.counts.into(),
            page: s.page.into(),
            version: Arc::new(AttachmentManagementVersion { inner: s.version }),
            redacted_diagnostics: diagnostics,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_totals_and_chat_scope_cross_the_boundary() {
        let counts: AttachmentJobCountsFfi = app::AttachmentJobCounts {
            active: 1024,
            complete: false,
            paused: 3,
            policy_blocked: 7,
            ..Default::default()
        }
        .into();
        assert_eq!(counts.active, 1024);
        assert!(!counts.complete);
        assert_eq!(counts.paused, 3);
        assert_eq!(counts.policy_blocked, 7);
        let q: app::AttachmentJobQuery = AttachmentJobQueryFfi {
            group_id_hex: "aa".into(),
            view: AttachmentJobViewFfi::NeedsAttention,
            origin: AttachmentJobOriginFfi::Explicit,
        }
        .into();
        assert_eq!(q.view, app::AttachmentJobView::NeedsAttention);
        assert_eq!(q.origin, app::AttachmentJobOrigin::Explicit);
    }
}
