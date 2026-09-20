//! Server-owned interactions are independent of terminal semantic turn records.
use crate::api::schema::*;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const MAX_RECORDS: usize = 4096;
const MAX_RECEIPTS: usize = 8192;

#[derive(Clone)]
pub(crate) struct InteractionManager(Arc<Inner>);
struct Inner {
    path: PathBuf,
    state: Mutex<State>,
    load_error: Option<String>,
}
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct State {
    revision: u64,
    #[serde(default)]
    reset_before_revision: u64,
    records: Vec<InteractionRecord>,
    receipts: Vec<InteractionReceipt>,
    #[serde(skip)]
    answers: BTreeMap<String, serde_json::Value>,
}

impl InteractionManager {
    pub(crate) fn load() -> Self {
        let path = crate::session::data_dir().join("agent-interactions.json");
        match Self::load_at(path.clone()) {
            Ok(manager) => manager,
            Err(error) => {
                tracing::error!(%error, "interaction journal unavailable");
                Self(Arc::new(Inner {
                    path,
                    state: Mutex::new(State::default()),
                    load_error: Some(error),
                }))
            }
        }
    }
    pub(crate) fn load_at(path: PathBuf) -> Result<Self, String> {
        let mut state: State = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| format!("invalid interaction journal: {e}"))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(error) => return Err(error.to_string()),
        };
        for record in &mut state.records {
            if record.report.state == InteractionState::Pending {
                state.revision += 1;
                record.revision = state.revision;
                record.report.state = InteractionState::OwnerLost;
            }
        }
        for receipt in &mut state.receipts {
            if receipt.state == InteractionDeliveryState::Queued {
                state.revision += 1;
                receipt.revision = state.revision;
                receipt.state = InteractionDeliveryState::OwnerLost;
            }
        }
        enforce_retention(&mut state)?;
        let manager = Self(Arc::new(Inner {
            path,
            state: Mutex::new(state),
            load_error: None,
        }));
        let state = manager
            .0
            .state
            .lock()
            .map_err(|_| "interaction lock poisoned")?;
        manager.persist(&state)?;
        drop(state);
        Ok(manager)
    }
    pub(crate) fn available(&self) -> Result<(), String> {
        self.0
            .load_error
            .as_ref()
            .map_or(Ok(()), |error| Err(error.clone()))
    }
    fn persist(&self, state: &State) -> Result<(), String> {
        self.available()?;
        let parent = self
            .0
            .path
            .parent()
            .ok_or("interaction journal has no parent")?;
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let temp = self
            .0
            .path
            .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let write = || -> std::io::Result<()> {
            let mut output = crate::platform::create_private_state_file(&temp)?;
            output.write_all(&serde_json::to_vec(state)?)?;
            output.sync_all()?;
            crate::platform::replace_file(&temp, &self.0.path)?;
            crate::platform::sync_parent_directory(parent)
        };
        let result = write().map_err(|e| e.to_string());
        if result.is_err() {
            let _ = std::fs::remove_file(temp);
        }
        result
    }
    fn mutate<T>(&self, change: impl FnOnce(&mut State) -> Result<T, String>) -> Result<T, String> {
        self.available()?;
        let mut current = self
            .0
            .state
            .lock()
            .map_err(|_| "interaction lock poisoned")?;
        let mut candidate = current.clone();
        let result = change(&mut candidate)?;
        self.persist(&candidate)?;
        *current = candidate;
        Ok(result)
    }
    pub(crate) fn report(
        &self,
        params: InteractionReportParams,
        executions: &crate::execution::ExecutionManager,
    ) -> Result<InteractionRecord, String> {
        executions.with_interaction_owner(
            &params.target.owner,
            params.native_capability.as_deref(),
            true,
            true,
            || self.report_validated(params.clone()),
        )
    }
    fn report_validated(
        &self,
        mut params: InteractionReportParams,
    ) -> Result<InteractionRecord, String> {
        validate_report(&params)?;
        params.native_capability = None;
        self.mutate(|state| {
            if let Some(previous) = state
                .records
                .iter()
                .find(|r| r.report.target == params.target)
            {
                if previous.report == params {
                    return Ok(previous.clone());
                }
                if (previous.report.state != InteractionState::Pending
                    && previous.report.state != params.state)
                    || params.event_revision <= previous.report.event_revision
                {
                    return Err("interaction_stale_report".into());
                }
                let mut expected = previous.report.clone();
                expected.event_revision = params.event_revision;
                expected.state = params.state.clone();
                if expected != params {
                    return Err("interaction_identity_or_payload_changed".into());
                }
            } else if params.state != InteractionState::Pending {
                return Err("interaction_initial_state_or_capacity".into());
            }
            state.revision += 1;
            let record = InteractionRecord {
                report: params,
                revision: state.revision,
            };
            state
                .records
                .retain(|r| r.report.target != record.report.target);
            state.records.push(record.clone());
            if record.report.state != InteractionState::Pending {
                for receipt in &mut state.receipts {
                    if receipt.target == record.report.target
                        && receipt.state == InteractionDeliveryState::Queued
                    {
                        state.revision += 1;
                        receipt.revision = state.revision;
                        receipt.state = InteractionDeliveryState::Rejected;
                        state.answers.remove(&receipt.response_id);
                    }
                }
            }
            enforce_retention(state)?;
            Ok(record)
        })
    }
    /// Terminal executions cannot retain live-looking questions after their producer disappears.
    pub(crate) fn reconcile(
        &self,
        executions: &crate::execution::ExecutionManager,
    ) -> Result<(), String> {
        self.available()?;
        let candidates: Vec<_> = self
            .0
            .state
            .lock()
            .map_err(|_| "interaction lock poisoned")?
            .records
            .iter()
            .filter(|r| r.report.state == InteractionState::Pending)
            .map(|r| r.report.target.clone())
            .collect();
        for target in candidates {
            match executions.with_interaction_owner(&target.owner, None, false, false, || Ok(())) {
                Ok(()) => continue,
                Err(error) if owner_is_lost(&error) => {}
                Err(error) => return Err(error),
            }
            self.mutate(|state| {
                if let Some(record) = state.records.iter_mut().find(|r| {
                    r.report.target == target && r.report.state == InteractionState::Pending
                }) {
                    state.revision += 1;
                    record.revision = state.revision;
                    record.report.state = InteractionState::OwnerLost;
                    for receipt in &mut state.receipts {
                        if receipt.target == target
                            && receipt.state == InteractionDeliveryState::Queued
                        {
                            state.revision += 1;
                            receipt.revision = state.revision;
                            receipt.state = InteractionDeliveryState::OwnerLost;
                            state.answers.remove(&receipt.response_id);
                        }
                    }
                }
                Ok(())
            })?;
        }
        Ok(())
    }
    pub(crate) fn read(&self, target: &InteractionTarget) -> Option<InteractionRecord> {
        self.0
            .state
            .lock()
            .ok()?
            .records
            .iter()
            .find(|r| &r.report.target == target)
            .cloned()
    }
    pub(crate) fn list(
        &self,
        after: u64,
    ) -> Result<(u64, bool, Vec<InteractionRecord>, Vec<InteractionReceipt>), String> {
        self.available()?;
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| "interaction lock poisoned")?;
        if after > state.revision {
            return Err("interaction_invalid_revision".into());
        }
        let reset = after < state.reset_before_revision;
        Ok((
            state.revision,
            reset,
            state
                .records
                .iter()
                .filter(|r| reset || r.revision > after)
                .cloned()
                .collect(),
            state
                .receipts
                .iter()
                .filter(|r| reset || r.revision > after)
                .cloned()
                .collect(),
        ))
    }
    pub(crate) fn respond(
        &self,
        params: InteractionRespondParams,
        executions: &crate::execution::ExecutionManager,
    ) -> Result<InteractionReceipt, String> {
        executions.with_interaction_owner(&params.target.owner, None, false, false, || {
            self.respond_validated(params.clone())
        })
    }
    fn respond_validated(
        &self,
        params: InteractionRespondParams,
    ) -> Result<InteractionReceipt, String> {
        valid_id(&params.response_id)?;
        if params.answer.is_null()
            || serde_json::to_vec(&params.answer)
                .map_err(|e| e.to_string())?
                .len()
                > 65_536
        {
            return Err("interaction_invalid_answer".into());
        }
        self.mutate(|state| {
            if let Some(receipt) = state
                .receipts
                .iter()
                .find(|r| r.response_id == params.response_id)
            {
                if receipt.target != params.target {
                    return Err("interaction_response_id_reused".into());
                }
                return if receipt.state != InteractionDeliveryState::Queued
                    || state.answers.get(&params.response_id) == Some(&params.answer)
                {
                    Ok(receipt.clone())
                } else {
                    Err("interaction_response_id_reused".into())
                };
            }
            let record = state
                .records
                .iter()
                .find(|r| r.report.target == params.target)
                .ok_or("interaction_not_found")?;
            if record.report.state != InteractionState::Pending {
                return Err("interaction_resolved".into());
            }
            validate_answer(&record.report, &params.answer)?;
            if state
                .receipts
                .iter()
                .any(|r| r.target == params.target && r.state == InteractionDeliveryState::Queued)
            {
                return Err("interaction_delivery_already_queued".into());
            }
            state.revision += 1;
            let receipt = InteractionReceipt {
                target: params.target,
                response_id: params.response_id,
                state: InteractionDeliveryState::Queued,
                revision: state.revision,
            };
            state
                .answers
                .insert(receipt.response_id.clone(), params.answer);
            state.receipts.push(receipt.clone());
            enforce_retention(state)?;
            Ok(receipt)
        })
    }
    pub(crate) fn deliveries(
        &self,
        params: &InteractionDeliveryTarget,
        executions: &crate::execution::ExecutionManager,
    ) -> Result<Vec<InteractionDelivery>, String> {
        executions.with_interaction_owner(
            &params.owner,
            params.native_capability.as_deref(),
            true,
            false,
            || {
                self.available()?;
                let state = self
                    .0
                    .state
                    .lock()
                    .map_err(|_| "interaction lock poisoned")?;
                Ok(state
                    .receipts
                    .iter()
                    .filter(|r| {
                        r.target.owner == params.owner
                            && r.state == InteractionDeliveryState::Queued
                    })
                    .filter_map(|receipt| {
                        state
                            .answers
                            .get(&receipt.response_id)
                            .map(|answer| InteractionDelivery {
                                receipt: receipt.clone(),
                                answer: answer.clone(),
                            })
                    })
                    .collect())
            },
        )
    }
    pub(crate) fn ack(
        &self,
        params: &InteractionAckParams,
        executions: &crate::execution::ExecutionManager,
    ) -> Result<InteractionReceipt, String> {
        executions.with_interaction_owner(
            &params.producer.owner,
            params.producer.native_capability.as_deref(),
            true,
            false,
            || self.ack_validated(params),
        )
    }
    fn ack_validated(&self, params: &InteractionAckParams) -> Result<InteractionReceipt, String> {
        self.mutate(|state| {
            let receipt = state
                .receipts
                .iter_mut()
                .find(|r| {
                    r.target.owner == params.producer.owner
                        && r.target.request_id == params.request_id
                        && r.response_id == params.response_id
                })
                .ok_or("interaction_delivery_not_found")?;
            let next = if params.accepted {
                InteractionDeliveryState::Accepted
            } else {
                InteractionDeliveryState::Rejected
            };
            if receipt.state == next {
                return Ok(receipt.clone());
            }
            if receipt.state != InteractionDeliveryState::Queued {
                return Err("interaction_delivery_already_resolved".into());
            }
            state.revision += 1;
            receipt.state = next;
            receipt.revision = state.revision;
            let result = receipt.clone();
            if params.accepted {
                let record = state
                    .records
                    .iter_mut()
                    .find(|r| r.report.target == result.target)
                    .ok_or("interaction_not_found")?;
                if !matches!(
                    record.report.state,
                    InteractionState::Pending | InteractionState::Answered
                ) {
                    return Err("interaction_resolved".into());
                }
                state.revision += 1;
                record.revision = state.revision;
                record.report.state = InteractionState::Answered;
            }
            state.answers.remove(&result.response_id);
            enforce_retention(state)?;
            Ok(result)
        })
    }
}
fn enforce_retention(state: &mut State) -> Result<(), String> {
    enforce_retention_with_limits(state, MAX_RECORDS, MAX_RECEIPTS)
}
fn enforce_retention_with_limits(
    state: &mut State,
    record_limit: usize,
    receipt_limit: usize,
) -> Result<(), String> {
    while state.records.len() > record_limit {
        let Some((index, revision)) = state
            .records
            .iter()
            .enumerate()
            .filter(|(_, record)| record.report.state != InteractionState::Pending)
            .min_by_key(|(_, record)| record.revision)
            .map(|(index, record)| (index, record.revision))
        else {
            return Err("interaction_record_capacity".into());
        };
        state.records.remove(index);
        state.reset_before_revision = state.reset_before_revision.max(revision);
    }
    while state.receipts.len() > receipt_limit {
        let Some((index, revision)) = state
            .receipts
            .iter()
            .enumerate()
            .filter(|(_, receipt)| receipt.state != InteractionDeliveryState::Queued)
            .min_by_key(|(_, receipt)| receipt.revision)
            .map(|(index, receipt)| (index, receipt.revision))
        else {
            return Err("interaction_receipt_capacity".into());
        };
        let removed = state.receipts.remove(index);
        state.answers.remove(&removed.response_id);
        state.reset_before_revision = state.reset_before_revision.max(revision);
    }
    Ok(())
}
fn owner_is_lost(error: &str) -> bool {
    matches!(
        error,
        "interaction_execution_not_found" | "interaction_owner_lost"
    )
}
fn valid_id(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        Err("interaction_invalid_identity".into())
    } else {
        Ok(())
    }
}
fn validate_report(params: &InteractionReportParams) -> Result<(), String> {
    for value in [
        &params.target.request_id,
        &params.target.owner.execution_id,
        &params.target.owner.pane_id,
        &params.target.owner.producer,
        &params.target.owner.session_id,
        &params.thread_id,
        &params.turn_id,
        &params.item_id,
    ] {
        valid_id(value)?;
    }
    if params.event_revision == 0
        || params.question_ids.len() > 32
        || params
            .question_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != params.question_ids.len()
        || (params.kind == InteractionKind::Waiting
            && !(1..=3).contains(&params.question_ids.len()))
        || (params.kind == InteractionKind::PlanDecision && !params.question_ids.is_empty())
    {
        return Err("interaction_invalid_revision_or_questions".into());
    }
    for value in &params.question_ids {
        valid_id(value)?;
    }
    let payload = params
        .payload
        .as_object()
        .ok_or("interaction_invalid_payload")?;
    let allowed: &[&str] = match params.kind {
        InteractionKind::Waiting => &["questions", "isBlocking", "autoResolutionMs"],
        InteractionKind::Async => &["id", "type", "text", "phase", "delivery", "questions"],
        InteractionKind::PlanDecision => &["id", "itemId", "revision", "markdown", "status"],
    };
    if payload.keys().any(|key| !allowed.contains(&key.as_str()))
        || serde_json::to_vec(payload)
            .map_err(|e| e.to_string())?
            .len()
            > 262_144
    {
        return Err("interaction_invalid_payload".into());
    }
    match params.kind {
        InteractionKind::Waiting | InteractionKind::Async => {
            let questions = payload
                .get("questions")
                .and_then(serde_json::Value::as_array)
                .ok_or("interaction_invalid_questions")?;
            if questions.is_empty() || questions.len() != params.question_ids.len() {
                return Err("interaction_invalid_questions".into());
            }
            for (index, question) in questions.iter().enumerate() {
                let question = question.as_object().ok_or("interaction_invalid_question")?;
                let fields: &[&str] = if params.kind == InteractionKind::Waiting {
                    &["id", "header", "question", "options", "isOther", "isSecret"]
                } else {
                    &["title", "options"]
                };
                if question.keys().any(|key| !fields.contains(&key.as_str())) {
                    return Err("interaction_invalid_question_field".into());
                }
                let text_key = if params.kind == InteractionKind::Waiting {
                    "question"
                } else {
                    "title"
                };
                if question
                    .get(text_key)
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|value| value.trim().is_empty() || value.len() > 16_384)
                {
                    return Err("interaction_invalid_question_text".into());
                }
                if params.kind == InteractionKind::Waiting {
                    let question_id = question
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or("interaction_invalid_question_identity")?;
                    if params.question_ids.get(index).map(String::as_str) != Some(question_id)
                        || question
                            .get("header")
                            .and_then(serde_json::Value::as_str)
                            .is_none_or(|value| {
                                value.trim().is_empty() || value.chars().count() > 12
                            })
                        || !question
                            .get("isOther")
                            .is_some_and(serde_json::Value::is_boolean)
                        || !question
                            .get("isSecret")
                            .is_some_and(serde_json::Value::is_boolean)
                    {
                        return Err("interaction_invalid_question_identity".into());
                    }
                }
                if let Some(options) = question.get("options").filter(|value| !value.is_null()) {
                    let options = options.as_array().ok_or("interaction_invalid_options")?;
                    if options.is_empty()
                        || (params.kind == InteractionKind::Waiting
                            && !(2..=3).contains(&options.len()))
                    {
                        return Err("interaction_invalid_options".into());
                    }
                    let mut option_values = std::collections::BTreeSet::new();
                    for option in options {
                        if params.kind == InteractionKind::Async {
                            let value = option
                                .as_str()
                                .filter(|value| !value.trim().is_empty() && value.len() <= 4096)
                                .ok_or("interaction_invalid_option")?;
                            if !option_values.insert(value) {
                                return Err("interaction_invalid_option".into());
                            }
                        } else {
                            let option = option.as_object().ok_or("interaction_invalid_option")?;
                            if option
                                .keys()
                                .any(|key| !["label", "description"].contains(&key.as_str()))
                                || !["label", "description"].iter().all(|key| {
                                    option
                                        .get(*key)
                                        .and_then(serde_json::Value::as_str)
                                        .is_some_and(|value| {
                                            !value.trim().is_empty() && value.len() <= 4096
                                        })
                                })
                            {
                                return Err("interaction_invalid_option".into());
                            }
                            if !option_values.insert(
                                option
                                    .get("label")
                                    .and_then(serde_json::Value::as_str)
                                    .expect("validated label"),
                            ) {
                                return Err("interaction_invalid_option".into());
                            }
                        }
                    }
                }
            }
            if params.kind == InteractionKind::Waiting {
                if payload
                    .get("isBlocking")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
                    || !payload
                        .get("autoResolutionMs")
                        .is_some_and(serde_json::Value::is_null)
                {
                    return Err("interaction_invalid_waiting_payload".into());
                }
            } else if payload.get("id").and_then(serde_json::Value::as_str)
                != Some(params.item_id.as_str())
                || payload.get("type").and_then(serde_json::Value::as_str) != Some("agentMessage")
                || payload.get("phase").and_then(serde_json::Value::as_str) != Some("final_answer")
                || payload.get("delivery").and_then(serde_json::Value::as_str) != Some("async")
            {
                return Err("interaction_invalid_async_payload".into());
            }
            if params.kind == InteractionKind::Async {
                let questions = payload["questions"]
                    .as_array()
                    .expect("validated questions");
                let expected = questions
                    .iter()
                    .map(|question| {
                        let question = question.as_object().expect("validated question");
                        std::iter::once(
                            question["title"]
                                .as_str()
                                .expect("validated title")
                                .to_string(),
                        )
                        .chain(
                            question
                                .get("options")
                                .and_then(serde_json::Value::as_array)
                                .into_iter()
                                .flatten()
                                .map(|option| {
                                    format!("- {}", option.as_str().expect("validated option"))
                                }),
                        )
                        .collect::<Vec<_>>()
                        .join("\n")
                    })
                    .collect::<Vec<_>>()
                    .join("\n\n");
                if payload.get("text").and_then(serde_json::Value::as_str)
                    != Some(expected.as_str())
                {
                    return Err("interaction_async_text_mismatch".into());
                }
            }
        }
        InteractionKind::PlanDecision => {
            if payload.get("id").and_then(serde_json::Value::as_str)
                != Some(params.target.request_id.as_str())
                || payload.get("itemId").and_then(serde_json::Value::as_str)
                    != Some(params.item_id.as_str())
                || payload
                    .get("revision")
                    .and_then(serde_json::Value::as_u64)
                    .is_none_or(|revision| revision == 0)
                || payload
                    .get("markdown")
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|markdown| markdown.trim().is_empty())
                || payload.get("status").and_then(serde_json::Value::as_str) != Some("pending")
            {
                return Err("interaction_invalid_plan".into());
            }
        }
    }
    Ok(())
}

fn validate_answer(
    report: &InteractionReportParams,
    answer: &serde_json::Value,
) -> Result<(), String> {
    match report.kind {
        InteractionKind::Async => {
            let object = answer
                .as_object()
                .ok_or("interaction_invalid_async_answer")?;
            if object.len() != 2
                || object
                    .keys()
                    .any(|key| !["questionId", "answer"].contains(&key.as_str()))
                || object
                    .get("questionId")
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|id| !report.question_ids.iter().any(|candidate| candidate == id))
                || object
                    .get("answer")
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|value| value.trim().is_empty())
            {
                return Err("interaction_invalid_async_answer".into());
            }
        }
        InteractionKind::PlanDecision => {
            if !matches!(answer.as_str(), Some("implement" | "fresh" | "stay")) {
                return Err("interaction_invalid_plan_answer".into());
            }
        }
        InteractionKind::Waiting => {
            let answers = answer
                .as_object()
                .filter(|object| object.len() == 1)
                .and_then(|object| object.get("answers"))
                .and_then(serde_json::Value::as_object)
                .ok_or("interaction_invalid_waiting_answer")?;
            let questions = report.payload["questions"]
                .as_array()
                .expect("validated waiting questions");
            if answers.len() != report.question_ids.len() {
                return Err("interaction_invalid_waiting_answer".into());
            }
            for (index, id) in report.question_ids.iter().enumerate() {
                let entries = answers
                    .get(id)
                    .and_then(serde_json::Value::as_object)
                    .filter(|entry| entry.len() == 1)
                    .and_then(|entry| entry.get("answers"))
                    .and_then(serde_json::Value::as_array)
                    .filter(|entries| {
                        entries.len() <= 2 && entries.iter().all(serde_json::Value::is_string)
                    })
                    .ok_or("interaction_invalid_waiting_answer")?;
                if entries.is_empty() {
                    continue;
                }
                let question = questions[index]
                    .as_object()
                    .expect("validated waiting question");
                let labels = question["options"]
                    .as_array()
                    .expect("validated waiting options")
                    .iter()
                    .map(|option| option["label"].as_str().expect("validated label"))
                    .collect::<Vec<_>>();
                let first = entries[0].as_str().expect("validated answer");
                let selected = labels.contains(&first);
                let note_valid = entries
                    .get(1)
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|note| note.starts_with("user_note: "));
                let freeform = question["isOther"].as_bool() == Some(true) && entries.len() == 1;
                if (!selected && !freeform) || (selected && !note_valid) {
                    return Err("interaction_invalid_waiting_answer".into());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn report() -> InteractionReportParams {
        InteractionReportParams {
            target: InteractionTarget {
                owner: InteractionOwner {
                    execution_id: "e".into(),
                    pane_id: "p".into(),
                    producer: "xcsh".into(),
                    session_id: "s".into(),
                    generation: 1,
                },
                request_id: "q".into(),
            },
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item_id: "item".into(),
            question_ids: vec!["question".into()],
            event_revision: 1,
            kind: InteractionKind::Async,
            payload: serde_json::json!({
                "id": "item",
                "type": "agentMessage",
                "text": "Where?",
                "phase": "final_answer",
                "delivery": "async",
                "questions": [{"title":"Where?"}]
            }),
            state: InteractionState::Pending,
            native_capability: None,
        }
    }
    fn path() -> PathBuf {
        std::env::temp_dir().join(format!("herdr-interaction-{}.json", uuid::Uuid::new_v4()))
    }
    #[test]
    fn queued_delivery_is_private_and_only_acknowledgement_accepts() {
        let file = path();
        let manager = InteractionManager::load_at(file.clone()).unwrap();
        let request = report();
        manager.report_validated(request.clone()).unwrap();
        let response = InteractionRespondParams {
            target: request.target.clone(),
            response_id: "reply".into(),
            answer: serde_json::json!({"questionId":"question","answer":"secret answer"}),
        };
        assert_eq!(
            manager.respond_validated(response.clone()).unwrap().state,
            InteractionDeliveryState::Queued
        );
        assert_eq!(
            manager.read(&request.target).unwrap().report.state,
            InteractionState::Pending
        );
        assert!(!std::fs::read_to_string(&file)
            .unwrap()
            .contains("secret answer"));
        assert_eq!(
            manager.respond_validated(response.clone()).unwrap().state,
            InteractionDeliveryState::Queued
        );
        let ack = InteractionAckParams {
            producer: InteractionDeliveryTarget {
                owner: request.target.owner.clone(),
                native_capability: None,
            },
            request_id: "q".into(),
            response_id: "reply".into(),
            accepted: true,
        };
        assert_eq!(
            manager.ack_validated(&ack).unwrap().state,
            InteractionDeliveryState::Accepted
        );
        assert_eq!(
            manager
                .respond_validated(InteractionRespondParams {
                    target: request.target.clone(),
                    response_id: "reply".into(),
                    answer: serde_json::json!({"questionId":"question","answer":"not retained"}),
                })
                .unwrap()
                .state,
            InteractionDeliveryState::Accepted
        );
        assert!(!manager
            .0
            .state
            .lock()
            .unwrap()
            .answers
            .contains_key("reply"));
        assert_eq!(
            manager.read(&request.target).unwrap().report.state,
            InteractionState::Answered
        );
        assert_eq!(
            manager.ack_validated(&ack).unwrap().state,
            InteractionDeliveryState::Accepted
        );
        std::fs::remove_file(file).unwrap();
    }

    #[test]
    fn rejected_acknowledgement_erases_the_private_answer() {
        let file = path();
        let manager = InteractionManager::load_at(file.clone()).unwrap();
        let request = report();
        manager.report_validated(request.clone()).unwrap();
        manager
            .respond_validated(InteractionRespondParams {
                target: request.target.clone(),
                response_id: "rejected".into(),
                answer: serde_json::json!({"questionId":"question","answer":"secret"}),
            })
            .unwrap();
        manager
            .ack_validated(&InteractionAckParams {
                producer: InteractionDeliveryTarget {
                    owner: request.target.owner,
                    native_capability: None,
                },
                request_id: request.target.request_id,
                response_id: "rejected".into(),
                accepted: false,
            })
            .unwrap();
        assert!(manager.0.state.lock().unwrap().answers.is_empty());
        assert!(!std::fs::read_to_string(&file).unwrap().contains("secret"));
        std::fs::remove_file(file).unwrap();
    }
    #[test]
    fn restart_closes_pending_and_reports_lost_delivery_without_reopening() {
        let file = path();
        let manager = InteractionManager::load_at(file.clone()).unwrap();
        let request = report();
        manager.report_validated(request.clone()).unwrap();
        manager
            .respond_validated(InteractionRespondParams {
                target: request.target.clone(),
                response_id: "reply".into(),
                answer: serde_json::json!({"questionId":"question","answer":"private"}),
            })
            .unwrap();
        let restarted = InteractionManager::load_at(file.clone()).unwrap();
        assert_eq!(
            restarted.read(&request.target).unwrap().report.state,
            InteractionState::OwnerLost
        );
        assert!(restarted.report_validated(request).is_err());
        assert!(restarted.0.state.lock().unwrap().answers.is_empty());
        assert_eq!(
            restarted.0.state.lock().unwrap().receipts[0].state,
            InteractionDeliveryState::OwnerLost
        );
        std::fs::remove_file(file).unwrap();
    }
    #[test]
    fn failed_persistence_does_not_install_an_admitted_request() {
        let root = path();
        std::fs::write(&root, "file, not directory").unwrap();
        let manager = InteractionManager(Arc::new(Inner {
            path: root.join("state.json"),
            state: Mutex::new(State::default()),
            load_error: None,
        }));
        assert!(manager.report_validated(report()).is_err());
        assert_eq!(manager.0.state.lock().unwrap().revision, 0);
        std::fs::remove_file(root).unwrap();
    }
    #[test]
    fn owner_exit_and_wrong_generation_cannot_deliver_answers() {
        let root = path();
        std::fs::create_dir_all(&root).unwrap();
        let executions = crate::execution::ExecutionManager::load_at(root.join("executions.json"));
        executions
            .admit_visible(&ExecutionStartParams {
                execution_id: "e".into(),
                cwd: std::env::temp_dir().to_string_lossy().into(),
                workspace_id: None,
                label: None,
                command: ExecutionCommand::Argv {
                    argv: vec!["true".into()],
                },
            })
            .unwrap();
        executions
            .attach_visible("e", 1, "p".into(), "tab".into(), None)
            .unwrap();
        let manager = InteractionManager::load_at(root.join("interactions.json")).unwrap();
        let mut request = report();
        request.target.owner.generation = 0;
        request.native_capability = executions.native_capability("e");
        manager.report(request.clone(), &executions).unwrap();
        let mut wrong = request.target.clone();
        wrong.owner.generation = 1;
        assert!(manager
            .respond(
                InteractionRespondParams {
                    target: wrong,
                    response_id: "wrong".into(),
                    answer: serde_json::json!("No")
                },
                &executions
            )
            .is_err());
        executions.finish_visible(1, None, Some("producer disappeared"));
        manager.reconcile(&executions).unwrap();
        assert_eq!(
            manager.read(&request.target).unwrap().report.state,
            InteractionState::OwnerLost
        );
        assert!(manager
            .respond(
                InteractionRespondParams {
                    target: request.target,
                    response_id: "late".into(),
                    answer: serde_json::json!("No")
                },
                &executions
            )
            .is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ordinary_producer_mutations_require_the_execution_capability() {
        let root = path();
        std::fs::create_dir_all(&root).unwrap();
        let executions = crate::execution::ExecutionManager::load_at(root.join("executions.json"));
        executions
            .admit_visible(&ExecutionStartParams {
                execution_id: "e".into(),
                cwd: std::env::temp_dir().to_string_lossy().into(),
                workspace_id: None,
                label: None,
                command: ExecutionCommand::Argv {
                    argv: vec!["true".into()],
                },
            })
            .unwrap();
        executions
            .attach_visible("e", 1, "p".into(), "tab".into(), None)
            .unwrap();
        let manager = InteractionManager::load_at(root.join("interactions.json")).unwrap();
        let mut request = report();
        request.target.owner.generation = 0;
        assert!(executions.native_capability("e").is_some());
        assert_eq!(
            manager.report(request.clone(), &executions).unwrap_err(),
            "interaction_producer_capability_missing"
        );
        request.native_capability = Some("wrong".into());
        assert_eq!(
            manager.report(request.clone(), &executions).unwrap_err(),
            "interaction_producer_capability_mismatch"
        );
        request.native_capability = executions.native_capability("e");
        manager.report(request, &executions).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn journal_payload_cannot_contain_answers_or_local_drafts() {
        let mut request = report();
        request.payload =
            serde_json::json!({"questions":[{"title":"Question", "draft":"private"}]});
        assert!(validate_report(&request).is_err());
    }

    #[test]
    fn only_absent_or_stopped_executions_are_classified_as_owner_loss() {
        assert!(owner_is_lost("interaction_execution_not_found"));
        assert!(owner_is_lost("interaction_owner_lost"));
        for error in [
            "interaction_stale_generation",
            "interaction_provenance_mismatch",
            "interaction_producer_capability_missing",
            "interaction_producer_capability_mismatch",
            "execution state lock poisoned",
            "disk full",
        ] {
            assert!(!owner_is_lost(error), "{error}");
        }
    }

    #[test]
    fn bounded_history_returns_an_explicit_reset_snapshot_to_stale_consumers() {
        let file = path();
        let manager = InteractionManager::load_at(file.clone()).unwrap();
        let mut first = report();
        first.state = InteractionState::Answered;
        let mut second = first.clone();
        second.target.request_id = "q2".into();
        second.item_id = "item2".into();
        second.payload["id"] = serde_json::json!("item2");
        let mut state = manager.0.state.lock().unwrap();
        state.revision = 2;
        state.records = vec![
            InteractionRecord {
                report: first,
                revision: 1,
            },
            InteractionRecord {
                report: second.clone(),
                revision: 2,
            },
        ];
        enforce_retention_with_limits(&mut state, 1, 1).unwrap();
        drop(state);
        let (revision, reset, records, receipts) = manager.list(0).unwrap();
        assert_eq!(revision, 2);
        assert!(reset);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].report.target, second.target);
        assert!(receipts.is_empty());
        let (_, reset, records, _) = manager.list(1).unwrap();
        assert!(!reset);
        assert_eq!(records.len(), 1);
        std::fs::remove_file(file).unwrap();
    }

    #[test]
    fn payload_identity_and_fixed_fields_are_validated() {
        let mut request = report();
        request.payload = serde_json::json!({
            "id": "item",
            "type": "agentMessage",
            "text": "Where?\n- Canada",
            "phase": "final_answer",
            "delivery": "async",
            "questions": [{"title": "Where?", "options": ["Canada"]}]
        });
        assert!(validate_report(&request).is_ok());
        request.payload["delivery"] = serde_json::json!("waiting");
        assert_eq!(
            validate_report(&request),
            Err("interaction_invalid_async_payload".into())
        );
    }

    #[test]
    fn waiting_async_and_plan_payloads_reject_malformed_contracts() {
        let mut waiting = report();
        waiting.kind = InteractionKind::Waiting;
        waiting.payload = serde_json::json!({
            "questions": [{
                "id": "question",
                "header": "Region",
                "question": "Where?",
                "options": [
                    {"label":"Canada (Recommended)","description":"Use Canada."},
                    {"label":"US","description":"Use the US."}
                ],
                "isOther": true,
                "isSecret": false
            }],
            "isBlocking": true,
            "autoResolutionMs": null
        });
        assert!(validate_report(&waiting).is_ok());
        waiting.payload["autoResolutionMs"] = serde_json::json!(1000);
        assert_eq!(
            validate_report(&waiting),
            Err("interaction_invalid_waiting_payload".into())
        );

        let mut asynchronous = report();
        asynchronous.question_ids = vec!["question".into(), "question".into()];
        asynchronous.payload["questions"] = serde_json::json!([
            {"title":"One"}, {"title":"Two"}
        ]);
        asynchronous.payload["text"] = serde_json::json!("One\n\nTwo");
        assert_eq!(
            validate_report(&asynchronous),
            Err("interaction_invalid_revision_or_questions".into())
        );

        let mut plan = report();
        plan.kind = InteractionKind::PlanDecision;
        plan.question_ids.clear();
        plan.target.request_id = "plan".into();
        plan.item_id = "plan-item".into();
        plan.payload = serde_json::json!({
            "id":"plan",
            "itemId":"plan-item",
            "revision":1,
            "markdown":"# Plan",
            "status":"pending"
        });
        assert!(validate_report(&plan).is_ok());
        assert!(validate_answer(&plan, &serde_json::json!("implement")).is_ok());
        assert_eq!(
            validate_answer(&plan, &serde_json::json!("later")),
            Err("interaction_invalid_plan_answer".into())
        );
        assert!(validate_answer(
            &report(),
            &serde_json::json!({"questionId":"question","answer":"Canada"})
        )
        .is_ok());
        assert!(validate_answer(&report(), &serde_json::json!("Canada")).is_err());
    }
}
