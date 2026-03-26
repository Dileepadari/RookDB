use std::io;

use crate::catalog::types::Catalog;

use super::{JoinType, NLJMode};
use super::condition::{JoinCondition, evaluate_conditions};
use super::scanner::TupleScanner;
use super::result::JoinResult;
use super::tuple::Tuple;

/// Nested Loop Join executor.
pub struct NLJExecutor {
    pub outer_table: String,
    pub inner_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub block_size: usize,
    pub mode: NLJMode,
}

impl NLJExecutor {
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        match self.mode {
            NLJMode::Simple => self.execute_simple(db, catalog),
            NLJMode::Block => self.execute_block(db, catalog),
        }
    }

    fn execute_simple(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut outer_scanner = TupleScanner::new(db, &self.outer_table, catalog)?;
        let mut inner_scanner = TupleScanner::new(db, &self.inner_table, catalog)?;

        let left_schema = outer_scanner.schema.clone();
        let right_schema = inner_scanner.schema.clone();

        let mut result = JoinResult::new(&left_schema, &right_schema, &self.outer_table, &self.inner_table);

        match self.join_type {
            JoinType::Cross => {
                while let Some(o) = outer_scanner.next_tuple() {
                    inner_scanner.reset();
                    while let Some(i) = inner_scanner.next_tuple() {
                        result.add(Tuple::merge(&o, &i));
                    }
                }
            }
            JoinType::Inner => {
                while let Some(o) = outer_scanner.next_tuple() {
                    inner_scanner.reset();
                    while let Some(i) = inner_scanner.next_tuple() {
                        if evaluate_conditions(&self.conditions, &o, &i) {
                            result.add(Tuple::merge(&o, &i));
                        }
                    }
                }
            }
            JoinType::LeftOuter => {
                while let Some(o) = outer_scanner.next_tuple() {
                    inner_scanner.reset();
                    let mut matched = false;
                    while let Some(i) = inner_scanner.next_tuple() {
                        if evaluate_conditions(&self.conditions, &o, &i) {
                            result.add(Tuple::merge(&o, &i));
                            matched = true;
                        }
                    }
                    if !matched {
                        let null_right = Tuple::null_tuple(&right_schema);
                        result.add(Tuple::merge(&o, &null_right));
                    }
                }
            }
            JoinType::RightOuter => {
                while let Some(i) = inner_scanner.next_tuple() {
                    outer_scanner.reset();
                    let mut matched = false;
                    while let Some(o) = outer_scanner.next_tuple() {
                        if evaluate_conditions(&self.conditions, &o, &i) {
                            result.add(Tuple::merge(&o, &i));
                            matched = true;
                        }
                    }
                    if !matched {
                        let null_left = Tuple::null_tuple(&left_schema);
                        result.add(Tuple::merge(&null_left, &i));
                    }
                }
            }
            JoinType::FullOuter => {
                let mut right_matched = std::collections::HashSet::new();

                while let Some(o) = outer_scanner.next_tuple() {
                    inner_scanner.reset();
                    let mut left_matched = false;
                    let mut j = 0;
                    while let Some(i) = inner_scanner.next_tuple() {
                        if evaluate_conditions(&self.conditions, &o, &i) {
                            result.add(Tuple::merge(&o, &i));
                            left_matched = true;
                            right_matched.insert(j);
                        }
                        j += 1;
                    }
                    if !left_matched {
                        let null_right = Tuple::null_tuple(&right_schema);
                        result.add(Tuple::merge(&o, &null_right));
                    }
                }
                
                inner_scanner.reset();
                let mut j = 0;
                while let Some(i) = inner_scanner.next_tuple() {
                    if !right_matched.contains(&j) {
                        let null_left = Tuple::null_tuple(&left_schema);
                        result.add(Tuple::merge(&null_left, &i));
                    }
                    j += 1;
                }
            }
        }

        Ok(result)
    }

    fn execute_block(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut outer_scanner = TupleScanner::new(db, &self.outer_table, catalog)?;
        let mut inner_scanner = TupleScanner::new(db, &self.inner_table, catalog)?;

        let left_schema = outer_scanner.schema.clone();
        let right_schema = inner_scanner.schema.clone();

        let mut result = JoinResult::new(&left_schema, &right_schema, &self.outer_table, &self.inner_table);

        let block_size_tuples = self.block_size * 100; // approximate tuples per block

        // We handle Inner, Cross, and LeftOuter fully. RightOuter/FullOuter will use inner buffering.
        match self.join_type {
            JoinType::Inner | JoinType::Cross | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                let mut right_matched = std::collections::HashSet::new();

                loop {
                    let mut chunk = Vec::new();
                    for _ in 0..block_size_tuples {
                        if let Some(t) = outer_scanner.next_tuple() {
                            chunk.push(t);
                        } else {
                            break;
                        }
                    }
                    if chunk.is_empty() {
                        break;
                    }

                    inner_scanner.reset();
                    let mut outer_matched = vec![false; chunk.len()];
                    let mut inner_idx = 0;

                    while let Some(i) = inner_scanner.next_tuple() {
                        for (idx, o) in chunk.iter().enumerate() {
                            if self.join_type == JoinType::Cross {
                                result.add(Tuple::merge(o, &i));
                                outer_matched[idx] = true;
                                right_matched.insert(inner_idx);
                            } else if evaluate_conditions(&self.conditions, o, &i) {
                                result.add(Tuple::merge(o, &i));
                                outer_matched[idx] = true;
                                right_matched.insert(inner_idx);
                            }
                        }
                        inner_idx += 1;
                    }

                    if self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter {
                        for (idx, o) in chunk.iter().enumerate() {
                            if !outer_matched[idx] {
                                let null_right = Tuple::null_tuple(&right_schema);
                                result.add(Tuple::merge(o, &null_right));
                            }
                        }
                    }
                }

                if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
                    inner_scanner.reset();
                    let mut inner_idx = 0;
                    while let Some(i) = inner_scanner.next_tuple() {
                        if !right_matched.contains(&inner_idx) {
                            let null_left = Tuple::null_tuple(&left_schema);
                            result.add(Tuple::merge(&null_left, &i));
                        }
                        inner_idx += 1;
                    }
                }
            }
        }

        Ok(result)
    }
}
