//! Pure, server-owned admission control for agent fan-out.
//!
//! This deliberately has no terminal or provider dependency. The runtime can
//! use its decisions to release queued dispatches, while the model remains
//! deterministic and unit-testable.

use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdmissionRequest {
    pub(crate) id: String,
    pub(crate) provider: String,
    pub(crate) pane_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdmissionDecision {
    Admitted,
    Queued { position: usize },
}

#[derive(Debug)]
pub(crate) struct AdmissionController {
    max_in_flight: usize,
    provider_limits: HashMap<String, usize>,
    running: HashMap<String, AdmissionRequest>,
    queued: VecDeque<AdmissionRequest>,
}

impl AdmissionController {
    pub(crate) fn new(max_in_flight: usize, provider_limits: HashMap<String, usize>) -> Self {
        Self {
            max_in_flight,
            provider_limits,
            running: HashMap::new(),
            queued: VecDeque::new(),
        }
    }

    pub(crate) fn submit(&mut self, request: AdmissionRequest) -> AdmissionDecision {
        if self.can_admit(&request) {
            self.running.insert(request.id.clone(), request);
            AdmissionDecision::Admitted
        } else {
            self.queued.push_back(request);
            AdmissionDecision::Queued {
                position: self.queued.len(),
            }
        }
    }

    pub(crate) fn release_for_pane(&mut self, pane_id: &str) -> Vec<AdmissionRequest> {
        let completed = self
            .running
            .values()
            .find(|request| request.pane_id == pane_id)
            .map(|request| request.id.clone());
        completed.map(|id| self.cancel(&id)).unwrap_or_default()
    }

    /// Releases a prompt that could not be written to its terminal and makes
    /// the next eligible queued prompt available for dispatch.
    pub(crate) fn cancel(&mut self, id: &str) -> Vec<AdmissionRequest> {
        if self.running.remove(id).is_none() {
            return Vec::new();
        }
        let Some(index) = self
            .queued
            .iter()
            .position(|request| self.can_admit(request))
        else {
            return Vec::new();
        };
        self.queued.remove(index).into_iter().collect()
    }

    /// Preserve a prompt whose target disappeared during dispatch. It stays
    /// server-owned and will be retried when a later slot becomes available.
    pub(crate) fn requeue_front(&mut self, request: AdmissionRequest) {
        self.queued.push_front(request);
    }

    fn can_admit(&self, request: &AdmissionRequest) -> bool {
        self.running.len() < self.max_in_flight
            && self
                .provider_limits
                .get(&request.provider)
                .is_none_or(|limit| {
                    self.running
                        .values()
                        .filter(|running| running.provider == request.provider)
                        .count()
                        < *limit
                })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: &str, provider: &str) -> AdmissionRequest {
        AdmissionRequest {
            id: id.into(),
            provider: provider.into(),
            pane_id: format!("pane-{id}"),
        }
    }

    #[test]
    fn admits_up_to_the_global_limit_then_queues() {
        let mut admission = AdmissionController::new(2, HashMap::new());
        assert_eq!(
            admission.submit(request("a", "openai")),
            AdmissionDecision::Admitted
        );
        assert_eq!(
            admission.submit(request("b", "openai")),
            AdmissionDecision::Admitted
        );
        assert_eq!(
            admission.submit(request("c", "openai")),
            AdmissionDecision::Queued { position: 1 }
        );
        assert_eq!(admission.queued.len(), 1);
    }

    #[test]
    fn release_respects_provider_budget_without_starving_other_providers() {
        let mut admission = AdmissionController::new(3, HashMap::from([("openai".into(), 1)]));
        assert_eq!(
            admission.submit(request("a", "openai")),
            AdmissionDecision::Admitted
        );
        assert_eq!(
            admission.submit(request("b", "openai")),
            AdmissionDecision::Queued { position: 1 }
        );
        assert_eq!(
            admission.submit(request("c", "anthropic")),
            AdmissionDecision::Admitted
        );
        assert_eq!(
            admission.release_for_pane("pane-a"),
            vec![request("b", "openai")]
        );
    }

    #[test]
    fn cancelled_dispatch_releases_the_next_eligible_prompt() {
        let mut admission = AdmissionController::new(1, HashMap::new());
        assert_eq!(
            admission.submit(request("a", "openai")),
            AdmissionDecision::Admitted
        );
        assert_eq!(
            admission.submit(request("b", "openai")),
            AdmissionDecision::Queued { position: 1 }
        );

        assert_eq!(admission.cancel("a"), vec![request("b", "openai")]);
    }
}
