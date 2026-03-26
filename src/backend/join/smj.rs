//! Sort-Merge Join executor with external sort and merge-join.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};

use crate::catalog::types::{Catalog, Column};
use crate::disk::create_page;
use crate::heap::insert_tuple;
use crate::page::PAGE_SIZE;
use crate::table::page_count;

use super::JoinType;
use super::condition::{JoinCondition, evaluate_conditions};
use super::scanner::TupleScanner;
use super::result::JoinResult;
use super::tuple::{ColumnValue, Tuple};

/// A sorted run file on disk.
pub struct SortRun {
    pub file_path: String,
    pub page_count: u32,
}

/// Sort-Merge Join executor.
pub struct SMJExecutor {
    pub left_table: String,
    pub right_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub memory_pages: usize,
}

struct MergeItem {
    tuple: Tuple,
    run_idx: usize,
    sort_col_idx: usize,
}

impl PartialEq for MergeItem {
    fn eq(&self, other: &Self) -> bool {
        let val_a = &self.tuple.values[self.sort_col_idx];
        let val_b = &other.tuple.values[self.sort_col_idx];
        val_a.eq_value(val_b)
    }
}
impl Eq for MergeItem {}

impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let val_a = &self.tuple.values[self.sort_col_idx];
        let val_b = &other.tuple.values[self.sort_col_idx];
        // We want a min-heap, so we reverse the ordering
        val_b.partial_cmp_values(val_a)
    }
}
impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl SMJExecutor {
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        // Ensure tmp directory exists
        fs::create_dir_all("database/tmp")?;

        let left_database = catalog.databases.get(db).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Database not found")
        })?;
        let left_schema = left_database.tables.get(&self.left_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Left table not found"))?
            .columns.clone();
        let right_schema = left_database.tables.get(&self.right_table)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Right table not found"))?
            .columns.clone();

        let left_sort_col = self.conditions.first()
            .map(|c| c.left_col.clone())
            .unwrap_or_default();
        let right_sort_col = self.conditions.first()
            .map(|c| c.right_col.clone())
            .unwrap_or_default();

        let left_runs = self.sort_relation(&self.left_table, db, catalog, &left_sort_col)?;
        let right_runs = self.sort_relation(&self.right_table, db, catalog, &right_sort_col)?;

        let left_merged = self.merge_all_runs(left_runs, &left_schema, &left_sort_col)?;
        let right_merged = self.merge_all_runs(right_runs, &right_schema, &right_sort_col)?;

        let result = self.merge_join(db, catalog, &left_merged, &right_merged, &left_schema, &right_schema, &left_sort_col, &right_sort_col)?;

        let _ = self.cleanup_temp_files();

        Ok(result)
    }

    fn sort_relation(&self, table: &str, db: &str, catalog: &Catalog, sort_col: &str) -> io::Result<Vec<SortRun>> {
        let mut scanner = TupleScanner::new(db, table, catalog)?;
        let schema = scanner.schema.clone();
        let mut runs = Vec::new();
        let mut run_id = 0u32;

        loop {
            let mut batch: Vec<Tuple> = Vec::new();
            let batch_limit = self.memory_pages * 100;

            for _ in 0..batch_limit {
                match scanner.next_tuple() {
                    Some(t) => batch.push(t),
                    None => break,
                }
            }

            if batch.is_empty() {
                break;
            }

            batch.sort_by(|a, b| {
                let va = a.get_field(sort_col);
                let vb = b.get_field(sort_col);
                match (va, vb) {
                    (Some(va), Some(vb)) => {
                        va.partial_cmp_values(vb).unwrap_or(Ordering::Equal)
                    }
                    _ => Ordering::Equal,
                }
            });

            let run_path = format!("database/tmp/sort_run_{}_{}.tmp", table, run_id);
            let mut run_file = OpenOptions::new()
                .create(true).write(true).read(true).truncate(true)
                .open(&run_path)?;

            let mut header = vec![0u8; PAGE_SIZE];
            header[0..4].copy_from_slice(&1u32.to_le_bytes());
            run_file.write_all(&header)?;
            run_file.flush()?;

            create_page(&mut run_file)?;

            for t in &batch {
                let bytes = self.serialize_tuple(t, &schema);
                insert_tuple(&mut run_file, &bytes)?;
            }

            let pc = page_count(&mut run_file)?;
            runs.push(SortRun {
                file_path: run_path,
                page_count: pc,
            });
            run_id += 1;
        }

        Ok(runs)
    }

    fn merge_all_runs(&self, mut runs: Vec<SortRun>, schema: &[Column], sort_col: &str) -> io::Result<SortRun> {
        if runs.is_empty() {
            let path = format!("database/tmp/empty_run_{}.tmp", uuid_simple());
            let mut file = OpenOptions::new()
                .create(true).write(true).read(true).truncate(true)
                .open(&path)?;
            let mut header = vec![0u8; PAGE_SIZE];
            header[0..4].copy_from_slice(&1u32.to_le_bytes());
            file.write_all(&header)?;
            file.flush()?;
            create_page(&mut file)?;
            let pc = page_count(&mut file)?;
            return Ok(SortRun { file_path: path, page_count: pc });
        }

        let max_merge_ways = 16;
        let sort_col_idx = schema.iter().position(|c| c.name == sort_col).unwrap_or(0);

        while runs.len() > 1 {
            let mut next_runs = Vec::new();
            for chunk in runs.chunks(max_merge_ways) {
                if chunk.len() == 1 {
                    next_runs.push(SortRun {
                        file_path: chunk[0].file_path.clone(),
                        page_count: chunk[0].page_count,
                    });
                    continue;
                }

                let mut scanners = Vec::new();
                for run in chunk {
                    let file = OpenOptions::new().read(true).write(true).open(&run.file_path)?;
                    scanners.push(TupleScanner::from_file(file, schema.to_vec())?);
                }

                let mut heap = BinaryHeap::new();
                for (i, scanner) in scanners.iter_mut().enumerate() {
                    if let Some(tuple) = scanner.next_tuple() {
                        heap.push(MergeItem { tuple, run_idx: i, sort_col_idx });
                    }
                }

                let merged_path = format!("database/tmp/merged_{}.tmp", uuid_simple());
                let mut merged_file = OpenOptions::new()
                    .create(true).write(true).read(true).truncate(true)
                    .open(&merged_path)?;

                let mut header = vec![0u8; PAGE_SIZE];
                header[0..4].copy_from_slice(&1u32.to_le_bytes());
                merged_file.write_all(&header)?;
                merged_file.flush()?;
                create_page(&mut merged_file)?;

                while let Some(min_item) = heap.pop() {
                    let bytes = self.serialize_tuple(&min_item.tuple, schema);
                    insert_tuple(&mut merged_file, &bytes)?;

                    if let Some(tuple) = scanners[min_item.run_idx].next_tuple() {
                        heap.push(MergeItem { tuple, run_idx: min_item.run_idx, sort_col_idx });
                    }
                }

                let pc = page_count(&mut merged_file)?;
                next_runs.push(SortRun { file_path: merged_path, page_count: pc });

                for run in chunk {
                    let _ = fs::remove_file(&run.file_path);
                }
            }
            runs = next_runs;
        }

        Ok(runs.into_iter().next().unwrap())
    }

    fn merge_join(
        &self,
        _db: &str,
        _catalog: &Catalog,
        left_run: &SortRun,
        right_run: &SortRun,
        left_schema: &[Column],
        right_schema: &[Column],
        left_sort_col: &str,
        right_sort_col: &str,
    ) -> io::Result<JoinResult> {
        let left_file = OpenOptions::new().read(true).write(true).open(&left_run.file_path)?;
        let right_file = OpenOptions::new().read(true).write(true).open(&right_run.file_path)?;

        let mut left_scanner = TupleScanner::from_file(left_file, left_schema.to_vec())?;
        let mut right_scanner = TupleScanner::from_file(right_file, right_schema.to_vec())?;

        let mut result = JoinResult::new(left_schema, right_schema, &self.left_table, &self.right_table);

        let mut current_left = left_scanner.next_tuple();
        let mut current_right = right_scanner.next_tuple();
        let mut right_group: Vec<(Tuple, bool)> = Vec::new();

        while let Some(l) = &current_left {
            let lv_opt = l.get_field(left_sort_col);

            if lv_opt.is_none() {
                if self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter {
                    let null_right = Tuple::null_tuple(right_schema);
                    result.add(Tuple::merge(l, &null_right));
                }
                current_left = left_scanner.next_tuple();
                continue;
            }
            let lv = lv_opt.unwrap();

            if !right_group.is_empty() && right_group[0].0.get_field(right_sort_col).unwrap().eq_value(lv) {
                let mut matched_l = false;
                for (r, r_matched) in &mut right_group {
                    if evaluate_conditions(&self.conditions, l, r) {
                        result.add(Tuple::merge(l, r));
                        matched_l = true;
                        *r_matched = true;
                    }
                }
                if (self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter) && !matched_l {
                    let null_right = Tuple::null_tuple(right_schema);
                    result.add(Tuple::merge(l, &null_right));
                }
                current_left = left_scanner.next_tuple();
                continue;
            } else if !right_group.is_empty() {
                if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
                    for (r, r_matched) in &right_group {
                        if !r_matched {
                            let null_left = Tuple::null_tuple(left_schema);
                            result.add(Tuple::merge(&null_left, r));
                        }
                    }
                }
                right_group.clear();
            }

            let mut advanced_right = false;
            while let Some(r) = &current_right {
                let rv_opt = r.get_field(right_sort_col);
                if rv_opt.is_none() {
                    if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
                        let null_left = Tuple::null_tuple(left_schema);
                        result.add(Tuple::merge(&null_left, r));
                    }
                    current_right = right_scanner.next_tuple();
                    continue;
                }
                let rv = rv_opt.unwrap();

                match lv.partial_cmp_values(rv) {
                    Some(Ordering::Less) => { break; }
                    Some(Ordering::Greater) => {
                        if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
                            let null_left = Tuple::null_tuple(left_schema);
                            result.add(Tuple::merge(&null_left, r));
                        }
                        current_right = right_scanner.next_tuple();
                    }
                    Some(Ordering::Equal) => {
                        right_group.clear();
                        let key_val = rv.clone();
                        right_group.push((r.clone(), false));
                        
                        current_right = right_scanner.next_tuple();
                        while let Some(next_r) = &current_right {
                            if let Some(next_rv) = next_r.get_field(right_sort_col) {
                                if next_rv.eq_value(&key_val) {
                                    right_group.push((next_r.clone(), false));
                                    current_right = right_scanner.next_tuple();
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        advanced_right = true;
                        break;
                    }
                    None => {
                        if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
                            let null_left = Tuple::null_tuple(left_schema);
                            result.add(Tuple::merge(&null_left, r));
                        }
                        current_right = right_scanner.next_tuple();
                    }
                }
            }

            if advanced_right {
                let mut matched_l = false;
                for (r, r_matched) in &mut right_group {
                    if evaluate_conditions(&self.conditions, l, r) {
                        result.add(Tuple::merge(l, r));
                        matched_l = true;
                        *r_matched = true;
                    }
                }
                if (self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter) && !matched_l {
                    let null_right = Tuple::null_tuple(right_schema);
                    result.add(Tuple::merge(l, &null_right));
                }
            } else {
                if self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter {
                    let null_right = Tuple::null_tuple(right_schema);
                    result.add(Tuple::merge(l, &null_right));
                }
            }

            current_left = left_scanner.next_tuple();
        }

        if !right_group.is_empty() && (self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter) {
            for (r, r_matched) in &right_group {
                if !r_matched {
                    let null_left = Tuple::null_tuple(left_schema);
                    result.add(Tuple::merge(&null_left, r));
                }
            }
        }
        
        if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
            while let Some(r) = current_right {
                let null_left = Tuple::null_tuple(left_schema);
                result.add(Tuple::merge(&null_left, &r));
                current_right = right_scanner.next_tuple();
            }
        }

        Ok(result)
    }

    fn serialize_tuple(&self, tuple: &Tuple, schema: &[Column]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (i, col) in schema.iter().enumerate() {
            if let Some(val) = tuple.values.get(i) {
                match col.data_type.as_str() {
                    "INT" => {
                        match val {
                            ColumnValue::Int(v) => bytes.extend_from_slice(&v.to_le_bytes()),
                            _ => bytes.extend_from_slice(&0i32.to_le_bytes()),
                        }
                    }
                    "TEXT" => {
                        match val {
                            ColumnValue::Text(s) => {
                                let mut text_bytes = s.as_bytes().to_vec();
                                if text_bytes.len() > 10 {
                                    text_bytes.truncate(10);
                                } else if text_bytes.len() < 10 {
                                    text_bytes.extend(vec![b' '; 10 - text_bytes.len()]);
                                }
                                bytes.extend_from_slice(&text_bytes);
                            }
                            _ => bytes.extend_from_slice(&[b' '; 10]),
                        }
                    }
                    _ => {}
                }
            }
        }
        bytes
    }

    fn cleanup_temp_files(&self) -> io::Result<()> {
        let tmp_dir = "database/tmp";
        if let Ok(entries) = fs::read_dir(tmp_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if entry.path().is_file() {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos)
}
