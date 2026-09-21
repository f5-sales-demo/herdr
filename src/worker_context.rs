use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::layout::PaneId;

pub(crate) const CAPABILITY_ENV_VAR: &str = "HERDR_CONTEXT_CAPABILITY";
pub(crate) const PAIRING_TTL: Duration = Duration::from_secs(60);
pub(crate) const LEASE_TTL: Duration = Duration::from_secs(15 * 60);

pub(crate) fn new_secret() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

pub(crate) fn verifier(secret: &str) -> String {
    format!("{:x}", Sha256::digest(secret.as_bytes()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Grant {
    verifier: String,
    pane_id: u32,
    consumer_id: Option<String>,
    expires_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerContextState {
    pairings: Vec<Grant>,
    leases: Vec<Grant>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerContextHandoffState {
    pub(crate) state: WorkerContextState,
    pub(crate) pane_verifiers: Vec<(u32, String)>,
}

impl WorkerContextState {
    pub(crate) fn issue(&mut self, pane_id: PaneId) -> String {
        self.prune_expired();
        let secret = new_secret();
        self.pairings.push(Grant {
            verifier: verifier(&secret),
            pane_id: pane_id.raw(),
            consumer_id: None,
            expires_at_ms: now_ms().saturating_add(PAIRING_TTL.as_millis() as u64),
        });
        secret
    }

    pub(crate) fn claim(&mut self, pairing_secret: &str, consumer_id: &str) -> Option<String> {
        self.prune_expired();
        let expected = verifier(pairing_secret);
        let index = self
            .pairings
            .iter()
            .position(|grant| grant.verifier == expected)?;
        let pairing = self.pairings.remove(index);
        self.leases
            .retain(|grant| grant.consumer_id.as_deref() != Some(consumer_id));
        let lease_secret = new_secret();
        self.leases.push(Grant {
            verifier: verifier(&lease_secret),
            pane_id: pairing.pane_id,
            consumer_id: Some(consumer_id.to_owned()),
            expires_at_ms: now_ms().saturating_add(LEASE_TTL.as_millis() as u64),
        });
        Some(lease_secret)
    }

    pub(crate) fn pane_for_pairing(&self, pairing_secret: &str) -> Option<PaneId> {
        let expected = verifier(pairing_secret);
        self.pairings
            .iter()
            .find(|grant| grant.verifier == expected && grant.expires_at_ms >= now_ms())
            .map(|grant| PaneId::from_raw(grant.pane_id))
    }

    pub(crate) fn pane_for_lease(
        &mut self,
        lease_secret: &str,
        consumer_id: &str,
    ) -> Option<PaneId> {
        self.prune_expired();
        let expected = verifier(lease_secret);
        self.leases
            .iter()
            .find(|grant| {
                grant.verifier == expected && grant.consumer_id.as_deref() == Some(consumer_id)
            })
            .map(|lease| PaneId::from_raw(lease.pane_id))
    }

    pub(crate) fn renew(&mut self, lease_secret: &str, consumer_id: &str) -> bool {
        let expected = verifier(lease_secret);
        let Some(lease) = self.leases.iter_mut().find(|grant| {
            grant.verifier == expected && grant.consumer_id.as_deref() == Some(consumer_id)
        }) else {
            return false;
        };
        lease.expires_at_ms = now_ms().saturating_add(LEASE_TTL.as_millis() as u64);
        true
    }

    pub(crate) fn revoke(&mut self, lease_secret: &str, consumer_id: &str) -> bool {
        let expected = verifier(lease_secret);
        let before = self.leases.len();
        self.leases.retain(|grant| {
            !(grant.verifier == expected && grant.consumer_id.as_deref() == Some(consumer_id))
        });
        before != self.leases.len()
    }

    pub(crate) fn revoke_pane(&mut self, pane_id: PaneId) {
        self.pairings.retain(|grant| grant.pane_id != pane_id.raw());
        self.leases.retain(|grant| grant.pane_id != pane_id.raw());
    }

    pub(crate) fn retain_live(&mut self, mut is_live: impl FnMut(PaneId) -> bool) {
        self.pairings
            .retain(|grant| is_live(PaneId::from_raw(grant.pane_id)));
        self.leases
            .retain(|grant| is_live(PaneId::from_raw(grant.pane_id)));
    }

    pub(crate) fn rebind_panes(&mut self, aliases: &HashMap<u32, PaneId>) {
        for grant in self.pairings.iter_mut().chain(self.leases.iter_mut()) {
            if let Some(pane_id) = aliases.get(&grant.pane_id) {
                grant.pane_id = pane_id.raw();
            }
        }
    }

    fn prune_expired(&mut self) {
        let now = now_ms();
        self.pairings.retain(|grant| grant.expires_at_ms >= now);
        self.leases.retain(|grant| grant.expires_at_ms >= now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairing_is_one_time_and_lease_is_consumer_bound() {
        let pane = PaneId::from_raw(7);
        let mut state = WorkerContextState::default();
        let pairing = state.issue(pane);
        let lease = state.claim(&pairing, "window-a").expect("claim");
        assert!(state.claim(&pairing, "window-a").is_none());
        assert_eq!(state.pane_for_lease(&lease, "window-a"), Some(pane));
        assert_eq!(state.pane_for_lease(&lease, "window-b"), None);
        assert!(state.revoke(&lease, "window-a"));
        assert_eq!(state.pane_for_lease(&lease, "window-a"), None);
    }

    #[test]
    fn pairing_and_leases_are_bound_to_one_server_state() {
        let pane = PaneId::from_raw(7);
        let mut issuing_server = WorkerContextState::default();
        let mut other_server = WorkerContextState::default();
        let pairing = issuing_server.issue(pane);
        assert!(other_server.claim(&pairing, "window-a").is_none());

        let lease = issuing_server.claim(&pairing, "window-a").expect("claim");
        assert_eq!(other_server.pane_for_lease(&lease, "window-a"), None);
        assert_eq!(
            issuing_server.pane_for_lease(&lease, "window-a"),
            Some(pane)
        );
    }

    #[test]
    fn successful_resolution_renews_the_lease() {
        let pane = PaneId::from_raw(7);
        let mut state = WorkerContextState::default();
        let pairing = state.issue(pane);
        let lease = state.claim(&pairing, "window-a").expect("claim");
        state.leases[0].expires_at_ms = now_ms().saturating_add(1);
        let previous_expiry = state.leases[0].expires_at_ms;

        assert_eq!(state.pane_for_lease(&lease, "window-a"), Some(pane));
        assert!(state.renew(&lease, "window-a"));
        assert!(state.leases[0].expires_at_ms > previous_expiry);
    }

    #[test]
    fn cold_restart_has_no_pairings_or_leases() {
        let pane = PaneId::from_raw(7);
        let mut old_server = WorkerContextState::default();
        let pairing = old_server.issue(pane);
        let lease = old_server.claim(&pairing, "window-a").expect("claim");

        let mut restarted_server = WorkerContextState::default();
        assert_eq!(restarted_server.pane_for_lease(&lease, "window-a"), None);
    }

    #[test]
    fn pane_rebind_and_revoke_follow_internal_identity() {
        let old = PaneId::from_raw(7);
        let moved = PaneId::from_raw(12);
        let mut state = WorkerContextState::default();
        let pairing = state.issue(old);
        let lease = state.claim(&pairing, "window-a").expect("claim");
        state.rebind_panes(&HashMap::from([(old.raw(), moved)]));
        assert_eq!(state.pane_for_lease(&lease, "window-a"), Some(moved));
        state.revoke_pane(moved);
        assert_eq!(state.pane_for_lease(&lease, "window-a"), None);
    }

    #[test]
    fn expired_grants_and_replaced_consumer_leases_are_rejected() {
        let pane = PaneId::from_raw(9);
        let mut state = WorkerContextState::default();
        let expired_pairing = state.issue(pane);
        state.pairings[0].expires_at_ms = 0;
        assert!(state.claim(&expired_pairing, "window-a").is_none());

        let first_pairing = state.issue(pane);
        let first_lease = state
            .claim(&first_pairing, "window-a")
            .expect("first claim");
        let second_pairing = state.issue(pane);
        let second_lease = state
            .claim(&second_pairing, "window-a")
            .expect("replacement claim");
        assert_eq!(state.pane_for_lease(&first_lease, "window-a"), None);
        assert_eq!(state.pane_for_lease(&second_lease, "window-a"), Some(pane));
        state.leases[0].expires_at_ms = 0;
        assert_eq!(state.pane_for_lease(&second_lease, "window-a"), None);
    }

    #[test]
    fn stored_state_contains_only_secret_verifiers() {
        let pane = PaneId::from_raw(3);
        let mut state = WorkerContextState::default();
        let pairing = state.issue(pane);
        let lease = state.claim(&pairing, "window-a").expect("claim");
        let serialized = serde_json::to_string(&state).expect("serialize state");
        assert!(!serialized.contains(&pairing));
        assert!(!serialized.contains(&lease));
        assert!(serialized.contains(&verifier(&lease)));
    }

    #[test]
    fn retain_live_removes_all_grants_for_closed_panes() {
        let live = PaneId::from_raw(1);
        let closed = PaneId::from_raw(2);
        let mut state = WorkerContextState::default();
        let live_pairing = state.issue(live);
        let closed_pairing = state.issue(closed);
        let closed_lease = state
            .claim(&closed_pairing, "window-closed")
            .expect("closed claim");
        state.retain_live(|pane_id| pane_id == live);
        assert!(state.claim(&live_pairing, "window-live").is_some());
        assert_eq!(state.pane_for_lease(&closed_lease, "window-closed"), None);
    }
}
