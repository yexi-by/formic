//! 分片计划：格式解析与校验的唯一所有者。
//!
//! 计划是 JSONL 文件，一行一个单元，两种分片形状：
//! - `{"unit": 1, "files": ["a.txt", "dir/b.txt"]}` —— 文件清单；
//! - `{"unit": 2, "file": "big.txt", "start": 100, "end": 200}` —— 行区间
//!   （1 起始，双端闭区间；end 允许超过文件行数，视为到文件尾）。
//!
//! 校验不变量：单元号是不重复的自然数；路径是数据根内相对路径、解析后不逃逸
//! 数据根、指向存在的文件；分片非空。错误定位到计划文件、行号与单元号。

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// 一个计划单元：自然编号 + 分片范围。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanUnit {
    pub unit: u64,
    pub shard: Shard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shard {
    /// 数据根内相对路径清单。
    Files(Vec<PathBuf>),
    /// 单文件行区间，1 起始，双端闭区间。
    Lines { file: PathBuf, start: u64, end: u64 },
}

/// 计划错误：自带计划文件、行号、单元号与原因。
#[derive(Debug)]
pub struct PlanError {
    pub plan: PathBuf,
    pub line: usize,
    pub unit: Option<u64>,
    pub reason: String,
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.unit {
            Some(unit) => write!(
                f,
                "计划文件 {} 第 {} 行（单元 {unit}）：{}",
                self.plan.display(),
                self.line,
                self.reason
            ),
            None => write!(
                f,
                "计划文件 {} 第 {} 行：{}",
                self.plan.display(),
                self.line,
                self.reason
            ),
        }
    }
}

impl std::error::Error for PlanError {}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLine {
    unit: u64,
    files: Option<Vec<PathBuf>>,
    file: Option<PathBuf>,
    start: Option<u64>,
    end: Option<u64>,
}

/// 读取并校验整个计划；返回按文件顺序排列的单元列表。
pub fn load(plan_path: &Path, data_root: &Path) -> Result<Vec<PlanUnit>, PlanError> {
    let err = |line: usize, unit: Option<u64>, reason: String| PlanError {
        plan: plan_path.to_path_buf(),
        line,
        unit,
        reason,
    };
    let text = fs::read_to_string(plan_path).map_err(|e| err(0, None, format!("无法读取：{e}")))?;
    let root = data_root.canonicalize().map_err(|e| {
        err(
            0,
            None,
            format!("数据根 {} 不可用：{e}", data_root.display()),
        )
    })?;

    let mut units = Vec::new();
    let mut seen = HashSet::new();
    let mut line_counts: HashMap<PathBuf, u64> = HashMap::new();

    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed: RawLine = serde_json::from_str(trimmed)
            .map_err(|e| err(line_no, None, format!("不是合法的计划行：{e}")))?;
        let unit = parsed.unit;
        if unit == 0 {
            return Err(err(line_no, None, "单元号必须是不小于 1 的自然数".into()));
        }
        if !seen.insert(unit) {
            return Err(err(line_no, Some(unit), "单元号重复".into()));
        }

        let shard = match (parsed.files, parsed.file, parsed.start, parsed.end) {
            (Some(files), None, None, None) => {
                if files.is_empty() {
                    return Err(err(line_no, Some(unit), "files 不能为空".into()));
                }
                for f in &files {
                    validate_file(&root, f).map_err(|r| err(line_no, Some(unit), r))?;
                }
                Shard::Files(files)
            }
            (None, Some(file), Some(start), Some(end)) => {
                if start == 0 {
                    return Err(err(line_no, Some(unit), "start 必须不小于 1".into()));
                }
                if end < start {
                    return Err(err(line_no, Some(unit), "end 不能小于 start".into()));
                }
                validate_file(&root, &file).map_err(|r| err(line_no, Some(unit), r))?;
                let count = line_count(&root, &file, &mut line_counts)
                    .map_err(|r| err(line_no, Some(unit), r))?;
                if start > count {
                    return Err(err(
                        line_no,
                        Some(unit),
                        format!(
                            "分片为空：start = {start} 超出文件 {} 的行数 {count}",
                            file.display()
                        ),
                    ));
                }
                Shard::Lines { file, start, end }
            }
            _ => {
                return Err(err(
                    line_no,
                    Some(unit),
                    "形状必须是 files（文件清单）或 file + start + end（行区间）其一".into(),
                ));
            }
        };
        units.push(PlanUnit { unit, shard });
    }

    if units.is_empty() {
        return Err(err(0, None, "计划不含任何单元".into()));
    }
    Ok(units)
}

/// 校验单个根内相对路径：非绝对、存在、是文件、解析后不逃逸数据根。
fn validate_file(root: &Path, rel: &Path) -> Result<(), String> {
    if rel.is_absolute() {
        return Err(format!(
            "{} 是绝对路径，计划只接受数据根内的相对路径",
            rel.display()
        ));
    }
    let canon = root
        .join(rel)
        .canonicalize()
        .map_err(|_| format!("{} 在数据根内不存在", rel.display()))?;
    if !canon.starts_with(root) {
        return Err(format!("{} 解析后逃逸数据根", rel.display()));
    }
    if !canon.is_file() {
        return Err(format!("{} 不是文件", rel.display()));
    }
    Ok(())
}

/// 统计文件行数，同一文件只读一次。
fn line_count(root: &Path, rel: &Path, cache: &mut HashMap<PathBuf, u64>) -> Result<u64, String> {
    if let Some(count) = cache.get(rel) {
        return Ok(*count);
    }
    let text = fs::read_to_string(root.join(rel))
        .map_err(|e| format!("无法读取 {}：{e}", rel.display()))?;
    let count = text.lines().count() as u64;
    cache.insert(rel.to_path_buf(), count);
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        _dir: tempfile::TempDir,
        root: PathBuf,
        plan: PathBuf,
    }

    fn fixture(plan_text: &str) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("a.txt"), "甲\n乙\n").unwrap();
        fs::write(root.join("big.txt"), "一\n二\n三\n四\n").unwrap();
        let plan = dir.path().join("plan.jsonl");
        fs::write(&plan, plan_text).unwrap();
        Fixture {
            _dir: dir,
            root,
            plan,
        }
    }

    #[test]
    fn files_shape_ok() {
        let f = fixture("{\"unit\": 1, \"files\": [\"a.txt\", \"big.txt\"]}\n");
        let units = load(&f.plan, &f.root).unwrap();
        assert_eq!(
            units,
            vec![PlanUnit {
                unit: 1,
                shard: Shard::Files(vec![PathBuf::from("a.txt"), PathBuf::from("big.txt")]),
            }]
        );
    }

    #[test]
    fn lines_shape_ok_end_may_exceed_file() {
        let f = fixture("{\"unit\": 2, \"file\": \"big.txt\", \"start\": 2, \"end\": 100}\n");
        let units = load(&f.plan, &f.root).unwrap();
        assert_eq!(
            units,
            vec![PlanUnit {
                unit: 2,
                shard: Shard::Lines {
                    file: PathBuf::from("big.txt"),
                    start: 2,
                    end: 100
                },
            }]
        );
    }

    #[test]
    fn duplicate_unit_rejected() {
        let f = fixture(
            "{\"unit\": 1, \"files\": [\"a.txt\"]}\n{\"unit\": 1, \"files\": [\"big.txt\"]}\n",
        );
        let e = load(&f.plan, &f.root).unwrap_err();
        assert_eq!(e.unit, Some(1));
        assert!(e.to_string().contains("单元 1"), "{e}");
    }

    #[test]
    fn unit_zero_rejected() {
        let f = fixture("{\"unit\": 0, \"files\": [\"a.txt\"]}\n");
        assert!(
            load(&f.plan, &f.root)
                .unwrap_err()
                .to_string()
                .contains("自然数")
        );
    }

    #[test]
    fn absolute_path_rejected() {
        let f = fixture("{\"unit\": 3, \"files\": [\"C:/Windows/win.ini\"]}\n");
        let e = load(&f.plan, &f.root).unwrap_err();
        assert!(e.to_string().contains("单元 3"), "{e}");
        assert!(e.to_string().contains("绝对路径"), "{e}");
    }

    #[test]
    fn escaping_path_rejected() {
        let f = fixture("{\"unit\": 4, \"files\": [\"../outside.txt\"]}\n");
        let e = load(&f.plan, &f.root).unwrap_err();
        assert!(e.to_string().contains("单元 4"), "{e}");
    }

    #[test]
    fn missing_file_rejected() {
        let f = fixture("{\"unit\": 5, \"files\": [\"nope.txt\"]}\n");
        let e = load(&f.plan, &f.root).unwrap_err();
        assert!(e.to_string().contains("不存在"), "{e}");
    }

    #[test]
    fn empty_files_rejected() {
        let f = fixture("{\"unit\": 6, \"files\": []}\n");
        assert!(
            load(&f.plan, &f.root)
                .unwrap_err()
                .to_string()
                .contains("不能为空")
        );
    }

    #[test]
    fn start_beyond_eof_is_empty_shard() {
        let f = fixture("{\"unit\": 7, \"file\": \"big.txt\", \"start\": 5, \"end\": 8}\n");
        let e = load(&f.plan, &f.root).unwrap_err();
        assert!(e.to_string().contains("分片为空"), "{e}");
        assert!(e.to_string().contains("单元 7"), "{e}");
    }

    #[test]
    fn bad_range_rejected() {
        let f = fixture("{\"unit\": 8, \"file\": \"big.txt\", \"start\": 3, \"end\": 2}\n");
        assert!(
            load(&f.plan, &f.root)
                .unwrap_err()
                .to_string()
                .contains("不能小于")
        );
    }

    #[test]
    fn mixed_shape_rejected() {
        let f = fixture("{\"unit\": 9, \"files\": [\"a.txt\"], \"start\": 1, \"end\": 2}\n");
        let e = load(&f.plan, &f.root).unwrap_err();
        assert!(e.to_string().contains("单元 9"), "{e}");
    }

    #[test]
    fn bad_json_reported_with_line() {
        let f = fixture("not json\n");
        let e = load(&f.plan, &f.root).unwrap_err();
        assert_eq!(e.line, 1);
        assert!(e.to_string().contains("第 1 行"), "{e}");
    }

    #[test]
    fn empty_plan_rejected() {
        let f = fixture("\n  \n");
        assert!(
            load(&f.plan, &f.root)
                .unwrap_err()
                .to_string()
                .contains("不含任何单元")
        );
    }
}
