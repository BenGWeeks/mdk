//! Authoritative final read eligibility for received timeline notifications.
use serde::{Deserialize, Serialize};
use storage_sqlite::MessageNotificationReadState;

/// Final read eligibility of one message for one account, for a host's
/// last check immediately before posting or enriching an OS notification.
///
/// Derived in one read transaction from the canonical timeline row and the
/// persisted read state (its canonical retained anchor, which survives
/// retention pruning of the marker row). It does not and cannot make the OS
/// post atomic: a message can still be read between this answer and the
/// post, so hosts must also cancel a posted notification when the message is
/// later read.
///
/// Variant order is part of the binding contract. Append new variants only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationReadEligibility {
    /// Unread past the persisted read marker.
    Unread,
    /// Read-tracked, but the conversation has no persisted read state yet,
    /// so no read marker can cover it.
    UnreadWithoutBaseline,
    /// The persisted read marker covers the message.
    Read,
    /// The canonical row is a deletion tombstone.
    Deleted,
    /// Convergence invalidated the canonical row.
    Invalidated,
    /// The account authored the message.
    OwnMessage,
    /// The sender is blocked.
    SenderBlocked,
    /// A timeline row outside received-chat read tracking, such as agent activity.
    /// It cannot authorize a received-chat notification.
    NotReadTracked,
    /// A raw event that is not a timeline row (for example a reaction). It is
    /// never evaluated as a chat message; reactions use their separate policy.
    NotTimelineMessage,
    /// No such message in this group for this account, including a message id
    /// that belongs to another group or account.
    Absent,
}

impl NotificationReadEligibility {
    /// Whether durable read state permits a post. `false` means the message
    /// is read, gone, retracted, own, blocked, or absent; the host must not
    /// post it. Cancel only a matching current notification, preserving newer
    /// unread messages in the same conversation.
    pub fn permits_post(self) -> bool {
        matches!(self, Self::Unread | Self::UnreadWithoutBaseline)
    }
}

impl From<MessageNotificationReadState> for NotificationReadEligibility {
    fn from(value: MessageNotificationReadState) -> Self {
        match value {
            MessageNotificationReadState::Unread => Self::Unread,
            MessageNotificationReadState::UnreadWithoutBaseline => Self::UnreadWithoutBaseline,
            MessageNotificationReadState::Read => Self::Read,
            MessageNotificationReadState::Deleted => Self::Deleted,
            MessageNotificationReadState::Invalidated => Self::Invalidated,
            MessageNotificationReadState::OwnMessage => Self::OwnMessage,
            MessageNotificationReadState::SenderBlocked => Self::SenderBlocked,
            MessageNotificationReadState::NotReadTracked => Self::NotReadTracked,
            MessageNotificationReadState::NotTimelineMessage => Self::NotTimelineMessage,
            MessageNotificationReadState::Absent => Self::Absent,
        }
    }
}
