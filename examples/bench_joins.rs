//! Benchmark suite for testing Join algorithm performance at scale.
//! Generates synthetic data and outputs performance metrics.

use std::fs::{self, OpenOptions};

use storage_manager::catalog::types::Column;
use storage_manager::catalog::{create_database, create_table, init_catalog, load_catalog, save_catalog};
use storage_manager::heap::insert_tuple;
use storage_manager::join::{JoinType, NLJMode};
use storage_manager::join::condition::{JoinCondition, JoinOp};
use storage_manager::join::nlj::NLJExecutor;
use storage_manager::join::smj::SMJExecutor;
use storage_manager::join::hj::HashJoinExecutor;
use storage_manager::join::metrics::JoinMetrics;

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

        for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::FullOuter] {
            println!("--- Join Type: {:?} ---", join_type);

            // 1. Simple NLJ
            let mut metrics_nlj = JoinMetrics::start("NLJ (Simple)");
            let nlj = NLJExecutor {
                outer_table: "table_a".to_string(),
                inner_table: "table_b".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                block_size: 2,
                mode: NLJMode::Simple,
            };
            let res_nlj = nlj.execute(&db, &catalog).unwrap();
            metrics_nlj.stop();
            metrics_nlj.tuples_output = res_nlj.tuples.len() as u64;
            metrics_nlj.display_row();

            // 2. Block NLJ
            let mut metrics_bnlj = JoinMetrics::start("NLJ (Block=50)");
            let bnlj = NLJExecutor {
                outer_table: "table_a".to_string(),
                inner_table: "table_b".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                block_size: 50,
                mode: NLJMode::Block,
            };
            let res_bnlj = bnlj.execute(&db, &catalog).unwrap();
            metrics_bnlj.stop();
            metrics_bnlj.tuples_output = res_bnlj.tuples.len() as u64;
            metrics_bnlj.display_row();

            // 3. Hash Join
            let mut metrics_hj = JoinMetrics::start("Hash Join");
            let hj = HashJoinExecutor {
                build_table: "table_b".to_string(),
                probe_table: "table_a".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                memory_pages: 5,
                num_partitions: 4,
            };
            let res_hj = hj.execute(&db, &catalog).unwrap();
            metrics_hj.stop();
            metrics_hj.tuples_output = res_hj.tuples.len() as u64;
            metrics_hj.display_row();

            // 4. Sort Merge Join
            let mut metrics_smj = JoinMetrics::start("Sort-Merge Join");
            let smj = SMJExecutor {
                left_table: "table_a".to_string(),
                right_table: "table_b".to_string(),
                conditions: condition.clone(),
                join_type: join_type.clone(),
                memory_pages: 5,
            };
            let res_smj = smj.execute(&db, &catalog).unwrap();
            metrics_smj.stop();
            metrics_smj.tuples_output = res_smj.tuples.len() as u64;
            metrics_smj.display_row();
            
            assert_eq!(res_nlj.tuples.len(), res_hj.tuples.len());
            assert_eq!(res_nlj.tuples.len(), res_smj.tuples.len());
            assert_eq!(res_nlj.tuples.len(), res_bnlj.tuples.len());
        }
    }
}
