//! 可恢复作业清单。内部摘要只验证本次 plan、task、schema 与 input 是否仍是同一
//! 作业；面向人的 CLI 不显示也不要求操作者传回摘要。

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::output::{OutputRoot, RecordFormat};
use crate::plan::PlanUnit;
use crate::structured::OutputContract;

const MANIFEST_FILE: &str = ".formic-job.json";
const TEMP_MANIFEST_FILE: &str = ".tmp-formic-job.json";
const STATE_FILE: &str = ".formic-job-state.jsonl";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UnitState {
    NotStarted,
    Started,
    Published,
    Failed,
    Stopped,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    plan_digest: String,
    task_digest: String,
    schema_digest: Option<String>,
    input_digest: String,
    output_format: String,
    units: Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateEvent {
    sequence: u64,
    unit: u64,
    state: UnitState,
}

pub struct Fingerprints {
    plan_digest: String,
    task_digest: String,
    schema_digest: Option<String>,
    input_digest: String,
    output_format: String,
}

pub struct JobState {
    root: OutputRoot,
    state_file: fs::File,
    states: BTreeMap<u64, UnitState>,
    next_sequence: u64,
}

pub struct ResumeSelection {
    pub already_completed: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("无法读取或写入作业状态 {path}：{source}")]
    StateIo { path: PathBuf, source: io::Error },
    #[error("输出目录已经属于一个 Formic 作业；继续时请使用 --resume")]
    ResumeRequired,
    #[error("输出目录没有可继续的 Formic 作业；请移除 --resume")]
    NoJob,
    #[error("作业状态文件无效；不能确认已发布结果和剩余单元")]
    InvalidState,
    #[error("当前 {0} 与首次运行不同；未发送模型请求")]
    InputChanged(&'static str),
    #[error("作业状态中的单元集合与当前计划不同；未发送模型请求")]
    UnitSetChanged,
    #[error("单元 {unit} 的已发布结果 {path} {reason}；请先恢复该文件")]
    CorruptPublished {
        unit: u64,
        path: PathBuf,
        reason: String,
    },
    #[error("单元 {unit} 的结果文件与作业状态冲突：{reason}")]
    ResultStateConflict { unit: u64, reason: String },
    #[error("结果目录含当前计划之外的完成记录 {0}")]
    UnexpectedResult(PathBuf),
    #[error("单元 {unit} 无法从状态 {state} 进入 {target}")]
    InvalidTransition {
        unit: u64,
        state: &'static str,
        target: &'static str,
    },
}

impl Fingerprints {
    pub fn from_snapshots(
        plan: &[u8],
        task: &[u8],
        schema: Option<&[u8]>,
        input_digest: String,
        format: RecordFormat,
    ) -> Self {
        Self {
            plan_digest: digest_bytes(plan),
            task_digest: digest_bytes(task),
            schema_digest: schema.map(digest_bytes),
            input_digest,
            output_format: format.extension().into(),
        }
    }
}

impl JobState {
    pub fn prepare(
        root: &OutputRoot,
        results: &OutputRoot,
        fingerprints: Fingerprints,
        units: &[PlanUnit],
        contract: &OutputContract,
        resume: bool,
    ) -> Result<(Self, ResumeSelection), JobError> {
        let manifest_path = Path::new(MANIFEST_FILE);
        let exists = root.exists(manifest_path);
        if exists && !resume {
            return Err(JobError::ResumeRequired);
        }
        if !exists && resume {
            return Err(JobError::NoJob);
        }

        let manifest = if exists {
            let bytes = root
                .read(manifest_path)
                .map_err(|source| JobError::StateIo {
                    path: root.display(manifest_path),
                    source,
                })?;
            serde_json::from_slice::<Manifest>(&bytes).map_err(|_| JobError::InvalidState)?
        } else {
            Manifest {
                plan_digest: fingerprints.plan_digest.clone(),
                task_digest: fingerprints.task_digest.clone(),
                schema_digest: fingerprints.schema_digest.clone(),
                input_digest: fingerprints.input_digest.clone(),
                output_format: fingerprints.output_format.clone(),
                units: units.iter().map(|unit| unit.unit).collect(),
            }
        };

        compare_fingerprints(&manifest, &fingerprints)?;
        let planned: Vec<u64> = units.iter().map(|unit| unit.unit).collect();
        if manifest.units != planned {
            return Err(JobError::UnitSetChanged);
        }

        let (states, next_sequence) = load_states(root, &manifest.units)?;
        // 先完整确认结果与当前状态一致。失败的首次启动不得留下 manifest；失败的
        // resume 也不得把旧 started 改写为 stopped。
        validate_results(results, &states, contract)?;

        if !exists {
            if root.exists(Path::new(STATE_FILE)) {
                return Err(JobError::InvalidState);
            }
            let bytes = serde_json::to_vec_pretty(&manifest).expect("作业清单可以序列化");
            root.write(Path::new(TEMP_MANIFEST_FILE), bytes)
                .and_then(|()| root.rename(Path::new(TEMP_MANIFEST_FILE), Path::new(MANIFEST_FILE)))
                .map_err(|source| JobError::StateIo {
                    path: root.display(Path::new(MANIFEST_FILE)),
                    source,
                })?;
        }

        let state_file =
            root.open_append(Path::new(STATE_FILE))
                .map_err(|source| JobError::StateIo {
                    path: root.display(Path::new(STATE_FILE)),
                    source,
                })?;
        let mut state = Self {
            root: root.clone(),
            state_file,
            states,
            next_sequence,
        };
        if resume {
            let interrupted: Vec<u64> = state
                .states
                .iter()
                .filter_map(|(&unit, &unit_state)| {
                    (unit_state == UnitState::Started).then_some(unit)
                })
                .collect();
            for unit in interrupted {
                state.append_transition(
                    unit,
                    &[UnitState::Started],
                    UnitState::Stopped,
                    "stopped",
                )?;
            }
        }
        let selection = ResumeSelection {
            already_completed: state
                .states
                .values()
                .filter(|state| **state == UnitState::Published)
                .count() as u64,
        };
        Ok((state, selection))
    }

    pub fn is_published(&self, unit: u64) -> bool {
        self.states.get(&unit) == Some(&UnitState::Published)
    }

    pub fn mark_started(&mut self, unit: u64) -> Result<(), JobError> {
        self.append_transition(
            unit,
            &[UnitState::NotStarted, UnitState::Failed, UnitState::Stopped],
            UnitState::Started,
            "started",
        )
    }

    pub fn mark_published(&mut self, unit: u64) -> Result<(), JobError> {
        self.append_transition(
            unit,
            &[UnitState::Started],
            UnitState::Published,
            "published",
        )
    }

    pub fn mark_failed(&mut self, unit: u64) -> Result<(), JobError> {
        self.append_transition(unit, &[UnitState::Started], UnitState::Failed, "failed")
    }

    pub fn mark_stopped(&mut self, unit: u64) -> Result<(), JobError> {
        self.append_transition(unit, &[UnitState::Started], UnitState::Stopped, "stopped")
    }

    fn append_transition(
        &mut self,
        unit: u64,
        allowed: &[UnitState],
        target: UnitState,
        target_name: &'static str,
    ) -> Result<(), JobError> {
        let state = self.states.get(&unit).ok_or(JobError::UnitSetChanged)?;
        if !allowed.contains(state) {
            return Err(JobError::InvalidTransition {
                unit,
                state: state_name(*state),
                target: target_name,
            });
        }
        let event = StateEvent {
            sequence: self.next_sequence,
            unit,
            state: target,
        };
        let line = format!(
            "{}\n",
            serde_json::to_string(&event).expect("作业状态事件可以序列化")
        );
        self.state_file
            .write_all(line.as_bytes())
            .and_then(|()| self.state_file.flush())
            .map_err(|source| JobError::StateIo {
                path: self.root.display(Path::new(STATE_FILE)),
                source,
            })?;
        self.states.insert(unit, target);
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(JobError::InvalidState)?;
        Ok(())
    }
}

fn compare_fingerprints(manifest: &Manifest, current: &Fingerprints) -> Result<(), JobError> {
    if manifest.plan_digest != current.plan_digest {
        return Err(JobError::InputChanged("plan"));
    }
    if manifest.task_digest != current.task_digest {
        return Err(JobError::InputChanged("task"));
    }
    if manifest.schema_digest != current.schema_digest {
        return Err(JobError::InputChanged("schema"));
    }
    if manifest.input_digest != current.input_digest {
        return Err(JobError::InputChanged("input"));
    }
    if manifest.output_format != current.output_format {
        return Err(JobError::InputChanged("输出格式"));
    }
    Ok(())
}

fn load_states(
    root: &OutputRoot,
    units: &[u64],
) -> Result<(BTreeMap<u64, UnitState>, u64), JobError> {
    let mut states: BTreeMap<u64, UnitState> = units
        .iter()
        .map(|unit| (*unit, UnitState::NotStarted))
        .collect();
    let state_path = Path::new(STATE_FILE);
    if !root.exists(state_path) {
        return Ok((states, 1));
    }
    let bytes = root.read(state_path).map_err(|source| JobError::StateIo {
        path: root.display(state_path),
        source,
    })?;
    let text = std::str::from_utf8(&bytes).map_err(|_| JobError::InvalidState)?;
    let mut expected_sequence = 1u64;
    for line in text.lines() {
        if line.trim().is_empty() {
            return Err(JobError::InvalidState);
        }
        let event: StateEvent = serde_json::from_str(line).map_err(|_| JobError::InvalidState)?;
        if event.sequence != expected_sequence {
            return Err(JobError::InvalidState);
        }
        let current = states.get_mut(&event.unit).ok_or(JobError::InvalidState)?;
        if !valid_transition(*current, event.state) {
            return Err(JobError::InvalidState);
        }
        *current = event.state;
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(JobError::InvalidState)?;
    }
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(JobError::InvalidState);
    }
    Ok((states, expected_sequence))
}

fn valid_transition(from: UnitState, to: UnitState) -> bool {
    matches!(
        (from, to),
        (
            UnitState::NotStarted | UnitState::Failed | UnitState::Stopped,
            UnitState::Started
        ) | (
            UnitState::Started,
            UnitState::Published | UnitState::Failed | UnitState::Stopped
        )
    )
}

fn validate_results(
    results: &OutputRoot,
    states: &BTreeMap<u64, UnitState>,
    contract: &OutputContract,
) -> Result<(), JobError> {
    let extension = contract.format().extension();
    for (&unit, &state) in states {
        let relative = PathBuf::from(format!("{unit}.{extension}"));
        let exists = results.exists(&relative);
        match (state, exists) {
            (UnitState::Published, true) => {
                let bytes = results
                    .read(&relative)
                    .map_err(|source| JobError::StateIo {
                        path: results.display(&relative),
                        source,
                    })?;
                contract
                    .validate_published_record(&bytes)
                    .map_err(|reason| JobError::CorruptPublished {
                        unit,
                        path: results.display(&relative),
                        reason,
                    })?;
            }
            (UnitState::Published, false) => {
                return Err(JobError::ResultStateConflict {
                    unit,
                    reason: "状态为 published，但结果文件缺失".into(),
                });
            }
            (_, true) => {
                return Err(JobError::ResultStateConflict {
                    unit,
                    reason: "存在结果文件，但状态不是 published".into(),
                });
            }
            (_, false) => {}
        }
    }

    for entry in results
        .read_dir(Path::new("."))
        .map_err(|source| JobError::StateIo {
            path: results.path().to_path_buf(),
            source,
        })?
    {
        let entry = entry.map_err(|source| JobError::StateIo {
            path: results.path().to_path_buf(),
            source,
        })?;
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            return Err(JobError::UnexpectedResult(
                results.display(Path::new(&entry.file_name())),
            ));
        }
        let name = entry.file_name();
        let path = Path::new(&name);
        if contract.is_structured() && path == Path::new("output-schema.json") {
            continue;
        }
        let Some(unit) = (path.extension().and_then(|value| value.to_str()) == Some(extension))
            .then(|| {
                path.file_stem()
                    .and_then(|value| value.to_str())
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .flatten()
        else {
            return Err(JobError::UnexpectedResult(results.display(path)));
        };
        if unit == 0 || !states.contains_key(&unit) {
            return Err(JobError::UnexpectedResult(results.display(path)));
        }
    }
    Ok(())
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn state_name(state: UnitState) -> &'static str {
    match state {
        UnitState::NotStarted => "not_started",
        UnitState::Started => "started",
        UnitState::Published => "published",
        UnitState::Failed => "failed",
        UnitState::Stopped => "stopped",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::Shard;

    fn fingerprints(label: &str) -> Fingerprints {
        Fingerprints {
            plan_digest: format!("plan-{label}"),
            task_digest: format!("task-{label}"),
            schema_digest: None,
            input_digest: format!("input-{label}"),
            output_format: "md".into(),
        }
    }

    #[test]
    fn task_fingerprint_uses_the_bytes_already_read_for_execution() {
        let directory = tempfile::tempdir().unwrap();
        let task_path = directory.path().join("task.md");
        fs::write(&task_path, "first task\n").unwrap();
        let task = fs::read_to_string(&task_path).unwrap();
        fs::write(&task_path, "second task\n").unwrap();

        let current = Fingerprints::from_snapshots(
            b"plan",
            task.as_bytes(),
            None,
            "input".into(),
            RecordFormat::Markdown,
        );

        assert_eq!(current.task_digest, digest_bytes(b"first task\n"));
        assert_ne!(current.task_digest, digest_bytes(b"second task\n"));
    }

    fn units(count: u64) -> Vec<PlanUnit> {
        (1..=count)
            .map(|unit| PlanUnit {
                unit,
                shard: Shard::Files(vec![PathBuf::from("a.txt")]),
            })
            .collect()
    }

    fn roots() -> (tempfile::TempDir, OutputRoot, OutputRoot) {
        let directory = tempfile::tempdir().unwrap();
        let out = directory.path().join("out");
        fs::create_dir(&out).unwrap();
        let root = OutputRoot::open(&out).unwrap();
        let results = root.create_subdir(Path::new("results")).unwrap();
        (directory, root, results)
    }

    #[test]
    fn resume_only_selects_unpublished_units_and_normalizes_interrupted_work() {
        let (_directory, root, results) = roots();
        let planned = units(3);
        let (mut state, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            false,
        )
        .unwrap();
        state.mark_started(1).unwrap();
        crate::output::publish(&results, 1, "done", RecordFormat::Markdown).unwrap();
        state.mark_published(1).unwrap();
        state.mark_started(2).unwrap();
        drop(state);

        let (state, selection) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            true,
        )
        .unwrap();
        assert_eq!(selection.already_completed, 1);
        assert!(state.is_published(1));
        assert!(!state.is_published(2));
        assert!(!state.is_published(3));
        let events = fs::read_to_string(root.display(Path::new(STATE_FILE))).unwrap();
        assert!(
            events
                .lines()
                .last()
                .unwrap()
                .contains("\"state\":\"stopped\"")
        );
    }

    #[test]
    fn resume_rejects_result_published_without_terminal_state_and_preserves_it() {
        let (_directory, root, results) = roots();
        let planned = units(1);
        let (mut state, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            false,
        )
        .unwrap();
        state.mark_started(1).unwrap();
        crate::output::publish(&results, 1, "immutable", RecordFormat::Markdown).unwrap();
        drop(state);
        let state_before = root.read(Path::new(STATE_FILE)).unwrap();

        let error = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            true,
        )
        .err()
        .expect("结果存在但缺 terminal state 时必须拒绝续跑");
        assert!(matches!(
            error,
            JobError::ResultStateConflict { unit: 1, .. }
        ));
        assert_eq!(results.read(Path::new("1.md")).unwrap(), b"immutable");
        assert_eq!(
            root.read(Path::new(STATE_FILE)).unwrap(),
            state_before,
            "拒绝 resume 前不得把 started 改成 stopped"
        );
    }

    #[test]
    fn first_run_rejects_existing_result_before_creating_job_identity() {
        for name in ["1.md", "unexpected.txt"] {
            let (_directory, root, results) = roots();
            results.write(Path::new(name), "preexisting").unwrap();

            let error = JobState::prepare(
                &root,
                &results,
                fingerprints("same"),
                &units(1),
                &OutputContract::Text,
                false,
            )
            .err()
            .expect("预存结果不能被认作新作业");
            assert!(matches!(
                error,
                JobError::ResultStateConflict { .. } | JobError::UnexpectedResult(_)
            ));
            assert!(!root.exists(Path::new(MANIFEST_FILE)));
            assert!(!root.exists(Path::new(STATE_FILE)));
        }
    }

    #[test]
    fn damaged_published_result_is_rejected_before_interrupted_state_changes() {
        let (_directory, root, results) = roots();
        let planned = units(2);
        let (mut state, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            false,
        )
        .unwrap();
        state.mark_started(1).unwrap();
        crate::output::publish(&results, 1, "valid", RecordFormat::Markdown).unwrap();
        state.mark_published(1).unwrap();
        state.mark_started(2).unwrap();
        drop(state);
        results.write(Path::new("1.md"), "").unwrap();
        let state_before = root.read(Path::new(STATE_FILE)).unwrap();

        let error = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            true,
        )
        .err()
        .expect("损坏的 published 结果必须阻止 resume");

        assert!(matches!(error, JobError::CorruptPublished { unit: 1, .. }));
        assert_eq!(root.read(Path::new(STATE_FILE)).unwrap(), state_before);
    }

    #[test]
    fn resume_rejects_changed_inputs_before_selecting_work() {
        let (_directory, root, results) = roots();
        let planned = units(1);
        let (_state, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("first"),
            &planned,
            &OutputContract::Text,
            false,
        )
        .unwrap();

        let error = JobState::prepare(
            &root,
            &results,
            fingerprints("changed"),
            &planned,
            &OutputContract::Text,
            true,
        )
        .err()
        .unwrap();
        assert!(matches!(error, JobError::InputChanged("plan")));
    }

    #[test]
    fn resume_checks_each_immutable_job_fingerprint() {
        let base = fingerprints("same");
        let manifest = Manifest {
            plan_digest: base.plan_digest.clone(),
            task_digest: base.task_digest.clone(),
            schema_digest: base.schema_digest.clone(),
            input_digest: base.input_digest.clone(),
            output_format: base.output_format.clone(),
            units: vec![1],
        };
        let mut changed = fingerprints("same");
        changed.plan_digest = "changed".into();
        assert!(matches!(
            compare_fingerprints(&manifest, &changed),
            Err(JobError::InputChanged("plan"))
        ));
        let mut changed = fingerprints("same");
        changed.task_digest = "changed".into();
        assert!(matches!(
            compare_fingerprints(&manifest, &changed),
            Err(JobError::InputChanged("task"))
        ));
        let mut changed = fingerprints("same");
        changed.schema_digest = Some("changed".into());
        assert!(matches!(
            compare_fingerprints(&manifest, &changed),
            Err(JobError::InputChanged("schema"))
        ));
        let mut changed = fingerprints("same");
        changed.input_digest = "changed".into();
        assert!(matches!(
            compare_fingerprints(&manifest, &changed),
            Err(JobError::InputChanged("input"))
        ));
        let mut changed = fingerprints("same");
        changed.output_format = "json".into();
        assert!(matches!(
            compare_fingerprints(&manifest, &changed),
            Err(JobError::InputChanged("输出格式"))
        ));
    }

    #[test]
    fn stopped_unit_can_resume_after_cancellation() {
        let (_directory, root, results) = roots();
        let planned = units(1);
        let (mut state, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            false,
        )
        .unwrap();
        state.mark_started(1).unwrap();
        state.mark_stopped(1).unwrap();
        drop(state);

        let (mut resumed, selection) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            true,
        )
        .unwrap();
        assert_eq!(selection.already_completed, 0);
        resumed.mark_started(1).unwrap();
    }

    #[test]
    fn failed_unit_remains_resumable_after_a_second_failure() {
        let (_directory, root, results) = roots();
        let planned = units(1);
        let (mut state, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            false,
        )
        .unwrap();
        state.mark_started(1).unwrap();
        state.mark_failed(1).unwrap();
        drop(state);

        let (mut second, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            true,
        )
        .unwrap();
        second.mark_started(1).unwrap();
        second.mark_failed(1).unwrap();
        drop(second);

        let (mut third, selection) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            true,
        )
        .unwrap();
        assert_eq!(selection.already_completed, 0);
        third.mark_started(1).unwrap();
    }

    #[test]
    fn resume_rejects_unknown_result_entries() {
        for name in ["foo.md", "2.txt", "0.md", "1.json"] {
            let (_directory, root, results) = roots();
            let planned = units(1);
            let (state, _) = JobState::prepare(
                &root,
                &results,
                fingerprints("same"),
                &planned,
                &OutputContract::Text,
                false,
            )
            .unwrap();
            drop(state);
            results.write(Path::new(name), "unexpected").unwrap();

            assert!(matches!(
                JobState::prepare(
                    &root,
                    &results,
                    fingerprints("same"),
                    &planned,
                    &OutputContract::Text,
                    true,
                ),
                Err(JobError::UnexpectedResult(_))
            ));
        }

        let (_directory, root, results) = roots();
        let planned = units(1);
        let (state, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("same"),
            &planned,
            &OutputContract::Text,
            false,
        )
        .unwrap();
        drop(state);
        fs::create_dir(results.path().join("nested")).unwrap();
        assert!(matches!(
            JobState::prepare(
                &root,
                &results,
                fingerprints("same"),
                &planned,
                &OutputContract::Text,
                true,
            ),
            Err(JobError::UnexpectedResult(_))
        ));
    }

    #[test]
    fn structured_resume_allows_only_the_schema_beside_numbered_results() {
        let (directory, root, results) = roots();
        let schema = directory.path().join("schema.json");
        fs::write(
            &schema,
            r#"{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}"#,
        )
        .unwrap();
        let contract = OutputContract::prepare(Some(&schema), &results).unwrap();
        let planned = units(1);
        let structured_fingerprints = || Fingerprints {
            plan_digest: "plan".into(),
            task_digest: "task".into(),
            schema_digest: Some("schema".into()),
            input_digest: "input".into(),
            output_format: "json".into(),
        };
        let (state, _) = JobState::prepare(
            &root,
            &results,
            structured_fingerprints(),
            &planned,
            &contract,
            false,
        )
        .unwrap();
        drop(state);
        JobState::prepare(
            &root,
            &results,
            structured_fingerprints(),
            &planned,
            &contract,
            true,
        )
        .expect("结构化 schema 记录是唯一允许的非编号文件");
    }

    #[test]
    fn append_log_grows_linearly_and_manifest_is_not_rewritten() {
        const UNIT_COUNT: u64 = 10_000;
        let (_directory, root, results) = roots();
        let planned = units(UNIT_COUNT);
        let (mut state, _) = JobState::prepare(
            &root,
            &results,
            fingerprints("large"),
            &planned,
            &OutputContract::Text,
            false,
        )
        .unwrap();
        let manifest_before = root.read(Path::new(MANIFEST_FILE)).unwrap();
        for unit in 1..=UNIT_COUNT {
            state.mark_started(unit).unwrap();
            state.mark_failed(unit).unwrap();
        }
        drop(state);

        assert_eq!(
            root.read(Path::new(MANIFEST_FILE)).unwrap(),
            manifest_before
        );
        let state_bytes = root.read(Path::new(STATE_FILE)).unwrap();
        assert_eq!(
            state_bytes.iter().filter(|byte| **byte == b'\n').count(),
            20_000
        );
        assert!(
            state_bytes.len() < 2 * 1024 * 1024,
            "追加日志不应重复写入全量单元 map：{} bytes",
            state_bytes.len()
        );
    }
}
