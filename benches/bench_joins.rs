//! Benchmark suite for testing Join algorithm performance at scale.
//! Generates synthetic data and outputs performance metrics.

use std::fs::{self, OpenOptions};
use std::time::Instant;

use storage_manager::catalog::types::Column;
use storage_manager::catalog::{create_database, create_table, init_catalog, load_catalog, save_catalog};
use storage_manager::heap::insert_tuple;
use storage_manager::join::{JoinType, JoinOp, NLJMode, JoinCondition};
use storage_manager::join::nlj::NLJExecutor;
use storage_manager::join::smj::SMJExecutor;
use storage_manager::join::hj::{HashJoinExecutor, HashJoinMode};
use storage_manager::join::shj::SymmetricHashJoinExecutor;
use storage_manager::join::direct::DirectJoinExecutor;

fn setup_bench_db(scale: i32) -> String {
    let db_name = format!("bench_joins_{}", scale);
    init_catalog();
    let mut catalog = load_catalog();
    catalog.databases.remove(&db_name);
    save_catalog(&catalog);

    let db_dir = format!("database/base/{}", db_name);
    let _ = fs::remove_dir_all(&db_dir);
    create_database(&mut catalog, &db_name);

    create_table(&mut catalog, &db_name, "table_a", vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "payload".to_string(), data_type: "TEXT".to_string() },
    ]);

    create_table(&mut catalog, &db_name, "table_b", vec![
        Column { name: "fk_id".to_string(), data_type: "INT".to_string() },
        Column { name: "payload".to_string(), data_type: "TEXT".to_string() },
    ]);

    let path_a = format!("database/base/{}/table_a.dat", db_name);
    let mut file_a = OpenOptions::new().read(true).write(true).open(&path_a).unwrap();
    for i in 1..=scale {
        insert_tuple(&mut file_a, &make_tuple(i, "AAAAA")).unwrap();
    }

    let path_b = format!("database/base/{}/table_b.dat", db_name);
    let mut file_b = OpenOptions::new().read(true).write(true).open(&path_b).unwrap();
    // 10% hit rate for join, remaining 90% misses to test outer joins
    for i in 1..=(scale/2) {
        let fk = if i % 10 == 0 { i } else { i + scale * 2 };
        insert_tuple(&mut file_b, &make_tuple(fk, "BBBBB")).unwrap();
    }

    let _ = fs::create_dir_all("database/tmp");
    db_name
}

fn make_tuple(id: i32, val: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&id.to_le_bytes());
    let mut txt = val.as_bytes().to_vec();
    if txt.len() > 10 { txt.truncate(10); }
    else if txt.len() < 10 { txt.extend(vec![b' '; 10 - txt.len()]); }
    bytes.extend_from_slice(&txt);
    bytes
}

fn bench_row(name: &str, elapsed_ms: f64, tuples: usize) {
    println!("{:<20} | {:<12.2} | {:<12}", name, elapsed_ms, tuples);
}

fn main() {
    println!("RookDB Join Benchmark Suite");
    println!("===========================");

    let sizes = [1000, 5000, 10000];
    let condition = vec![JoinCondition {
        left_table: "table_a".to_string(),
        left_col: "id".to_string(),
        operator: JoinOp::Eq,
        right_table: "table_b".to_string(),
        right_col: "fk_id".to_string(),
    }];

    for &size in &sizes {
        println!("\nGenerating dataset scale N = {}...", size);
        let db = setup_bench_db(size);
        let catalog = load_catalog();

        println!("{:<20} | {:<12} | {:<12}", "Algorithm", "Time (ms)", "Tuples Out");
        println!("---------------------------------------------------");

        let all_joins = [
            JoinType::Inner,
            JoinType::LeftOuter,
            JoinType::RightOuter,
            JoinType::FullOuter,
            JoinType::Cross,
            JoinType::SemiJoin,
            JoinType::AntiJoin,
            JoinType::Natural,
        ];

        for join_type in all_joins {
            println!("--- Join Type: {:?} ---", join_type);

            // 1. Simple NLJ
            let start = Instant::now();
            let nlj = NLJExecutor {
                outer_table: "table_a".to_string(),
                inner_table: "table_b".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                block_size: 2,
                mode: NLJMode::Simple,
            };
            match nlj.execute(&db, &catalog) {
                Ok(res) => bench_row("NLJ (Simple)", start.elapsed().as_secs_f64() * 1000.0, res.tuples.len()),
                Err(_) => println!("{:<20} | {:<12} | {:<12}", "NLJ (Simple)", "Unsupported", "-"),
            }

            // 2. Block NLJ
            let start = Instant::now();
            let bnlj = NLJExecutor {
                outer_table: "table_a".to_string(),
                inner_table: "table_b".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                block_size: 50,
                mode: NLJMode::Block,
            };
            match bnlj.execute(&db, &catalog) {
                Ok(res) => bench_row("NLJ (Block=50)", start.elapsed().as_secs_f64() * 1000.0, res.tuples.len()),
                Err(_) => println!("{:<20} | {:<12} | {:<12}", "NLJ (Block=50)", "Unsupported", "-"),
            }

            // 3. Grace Hash Join
            let start = Instant::now();
            let hj = HashJoinExecutor {
                build_table: "table_b".to_string(),
                probe_table: "table_a".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                mode: HashJoinMode::Grace,
                memory_pages: 5,
                num_partitions: 4,
            };
            match hj.execute(&db, &catalog) {
                Ok(res) => bench_row("Grace Hash Join", start.elapsed().as_secs_f64() * 1000.0, res.tuples.len()),
                Err(_) => println!("{:<20} | {:<12} | {:<12}", "Grace Hash Join", "Unsupported", "-"),
            }

            // 4. Hybrid Hash Join
            let start = Instant::now();
            let hhj = HashJoinExecutor {
                build_table: "table_b".to_string(),
                probe_table: "table_a".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                mode: HashJoinMode::Hybrid,
                memory_pages: 5,
                num_partitions: 4,
            };
            match hhj.execute(&db, &catalog) {
                Ok(res) => bench_row("Hybrid Hash Join", start.elapsed().as_secs_f64() * 1000.0, res.tuples.len()),
                Err(_) => println!("{:<20} | {:<12} | {:<12}", "Hybrid Hash Join", "Unsupported", "-"),
            }

            // 5. Symmetric Hash Join
            let start = Instant::now();
            let shj = SymmetricHashJoinExecutor {
                left_table: "table_a".to_string(),
                right_table: "table_b".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
            };
            match shj.execute(&db, &catalog) {
                Ok(res) => bench_row("Symmetric Hash", start.elapsed().as_secs_f64() * 1000.0, res.tuples.len()),
                Err(_) => println!("{:<20} | {:<12} | {:<12}", "Symmetric Hash", "Unsupported", "-"),
            }

            // 6. Sort Merge Join
            let start = Instant::now();
            let smj = SMJExecutor {
                left_table: "table_a".to_string(),
                right_table: "table_b".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                memory_pages: 5,
            };
            match smj.execute(&db, &catalog) {
                Ok(res) => bench_row("Sort-Merge Join", start.elapsed().as_secs_f64() * 1000.0, res.tuples.len()),
                Err(_) => println!("{:<20} | {:<12} | {:<12}", "Sort-Merge Join", "Unsupported", "-"),
            }

            // 7. Direct Join
            let start = Instant::now();
            let dj = DirectJoinExecutor {
                outer_table: "table_a".to_string(),
                inner_table: "table_b".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
            };
            match dj.execute(&db, &catalog) {
                Ok(res) => bench_row("Direct Join", start.elapsed().as_secs_f64() * 1000.0, res.tuples.len()),
                Err(_) => println!("{:<20} | {:<12} | {:<12}", "Direct Join", "Unsupported", "-"),
            }
        }
    }
}
