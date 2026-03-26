//! Integration tests for Outer Joins (Left, Right, Full).
//! Validates parity across NLJ, SMJ, and Hash Join.

use std::fs::{self, OpenOptions};
use std::sync::{Mutex, OnceLock};

use storage_manager::catalog::types::Column;
use storage_manager::catalog::{create_database, create_table, init_catalog, load_catalog, save_catalog};
use storage_manager::heap::insert_tuple;
use storage_manager::join::{JoinType, NLJMode};
use storage_manager::join::condition::{JoinCondition, JoinOp};
use storage_manager::join::nlj::NLJExecutor;
use storage_manager::join::smj::SMJExecutor;
use storage_manager::join::hj::HashJoinExecutor;

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn setup_test_db() -> String {
    let db_name = "test_outer_db";
    init_catalog();
    let mut catalog = load_catalog();
    catalog.databases.remove(db_name);
    save_catalog(&catalog);

    let db_dir = format!("database/base/{}", db_name);
    let _ = fs::remove_dir_all(&db_dir);

    create_database(&mut catalog, db_name);

    create_table(&mut catalog, db_name, "t1", vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "val".to_string(), data_type: "TEXT".to_string() },
    ]);

    create_table(&mut catalog, db_name, "t2", vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "val".to_string(), data_type: "TEXT".to_string() },
    ]);

    let t1_path = format!("database/base/{}/t1.dat", db_name);
    let mut t1_file = OpenOptions::new().read(true).write(true).open(&t1_path).unwrap();
    insert_tuple(&mut t1_file, &make_tuple(1, "A")).unwrap();
    insert_tuple(&mut t1_file, &make_tuple(2, "B")).unwrap();

    let t2_path = format!("database/base/{}/t2.dat", db_name);
    let mut t2_file = OpenOptions::new().read(true).write(true).open(&t2_path).unwrap();
    insert_tuple(&mut t2_file, &make_tuple(2, "X")).unwrap();
    insert_tuple(&mut t2_file, &make_tuple(3, "Y")).unwrap();

    let _ = fs::create_dir_all("database/tmp");

    db_name.to_string()
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

#[test]
fn test_outer_joins() {
    let _guard = test_lock().lock().unwrap();
    let db = setup_test_db();
    let catalog = load_catalog();

    let condition = vec![JoinCondition {
        left_table: "t1".to_string(),
        left_col: "id".to_string(),
        operator: JoinOp::Eq,
        right_table: "t2".to_string(),
        right_col: "id".to_string(),
    }];

    for join_type in [JoinType::LeftOuter, JoinType::RightOuter, JoinType::FullOuter] {
        let nlj = NLJExecutor {
            outer_table: "t1".to_string(),
            inner_table: "t2".to_string(),
            conditions: condition.clone(),
            join_type: join_type.clone(),
            block_size: 2,
            mode: NLJMode::Simple,
        };
        let nlj_result = nlj.execute(&db, &catalog).unwrap();

        let block_nlj = NLJExecutor {
            outer_table: "t1".to_string(),
            inner_table: "t2".to_string(),
            conditions: condition.clone(),
            join_type: join_type.clone(),
            block_size: 2,
            mode: NLJMode::Block,
        };
        let block_nlj_result = block_nlj.execute(&db, &catalog).unwrap();

        let smj = SMJExecutor {
            left_table: "t1".to_string(),
            right_table: "t2".to_string(),
            conditions: condition.clone(),
            join_type: join_type.clone(),
            memory_pages: 10,
        };
        let smj_result = smj.execute(&db, &catalog).unwrap();

        let hj = HashJoinExecutor {
            build_table: "t2".to_string(),
            probe_table: "t1".to_string(),
            conditions: condition.clone(),
            join_type: join_type.clone(),
            memory_pages: 10,
            num_partitions: 2,
        };
        let hj_result = hj.execute(&db, &catalog).unwrap();

        let expected_count = match join_type {
            JoinType::LeftOuter => 2,
            JoinType::RightOuter => 2,
            JoinType::FullOuter => 3,
            _ => 0,
        };

        assert_eq!(nlj_result.tuples.len(), expected_count, "NLJ Simple count mismatch for {:?}", join_type);
        assert_eq!(block_nlj_result.tuples.len(), expected_count, "NLJ Block count mismatch for {:?}", join_type);
        assert_eq!(smj_result.tuples.len(), expected_count, "SMJ count mismatch for {:?}", join_type);
        assert_eq!(hj_result.tuples.len(), expected_count, "HJ count mismatch for {:?}", join_type);
    }
}
