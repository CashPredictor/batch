# runner_batch.py
import sqlite3
import argparse
import copy
import logging
import os
from datetime import datetime
import pandas as pd
import yaml
import first_model
from first_model import get_train_test_params
from build_dataset_db import resolve_clients
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
        
def save_model_run(results_file, config, operation_mode, avg_accuracy):
    row_data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "model_path": config["first_model_params"]["model_path"],
        "database": config["first_model_params"]["database"],
        "test_database": config["first_model_params"].get("test_database"),
        "operation_mode": operation_mode,
        "avg_accuracy": avg_accuracy,
    }
    df_new = pd.DataFrame([row_data])
    if not os.path.exists(results_file):
        df_new.to_excel(results_file, index=False, sheet_name="runs")
        logging.info("Created results file: %s", results_file)
    else:
        existing_df = pd.read_excel(results_file, sheet_name="runs")
        combined_df = pd.concat([existing_df, df_new], ignore_index=True)
        combined_df.to_excel(results_file, index=False, sheet_name="runs")
        logging.info("Appended run to results file: %s", results_file)

def get_all_clients_from_db(db_path: str, table_name: str = "dataset") -> list[int]:
    conn = sqlite3.connect(db_path)
    try:
        query = f"""
            SELECT DISTINCT INVO_CLNTNO
            FROM {table_name}
            WHERE INVO_CLNTNO IS NOT NULL
            ORDER BY INVO_CLNTNO
        """
        df = pd.read_sql_query(query, conn)
        return df["INVO_CLNTNO"].astype(int).tolist()
    finally:
        conn.close()    
        
def parse_client_ids(s):
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    return [int(x) for x in s.split(",") if x.strip()]
    
def ensure_list(client_ids):
    if client_ids is None:
        return []
    if isinstance(client_ids, list):
        return client_ids
    return [client_ids]
    
def validate_db_path(db_path: str) -> str:
    if not db_path:
        raise ValueError("Musisz podać --db-path")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Baza nie istnieje: {db_path}")
    return db_path
    
def prepare_config_for_run(
    config,
    train_client_ids,
    test_client_ids,
    model_path,
    db_path,
):
    cfg = copy.deepcopy(config)
    cfg["first_model_params"]["database"] = db_path
    cfg["first_model_params"]["test_database"] = db_path
    cfg["first_model_params"]["model_path"] = model_path
    cfg["first_model_params"]["train_client_id"] = train_client_ids
    cfg["first_model_params"]["test_client_id"] = test_client_ids    
    return cfg
    
def run_phase(
    config,
    t0,
    operation_mode,
    train_client_ids,
    test_client_ids,
    model_path,
    db_path,
    results_file,
):
    cfg = prepare_config_for_run(
        config=config,
        train_client_ids=train_client_ids,
        test_client_ids=test_client_ids,
        model_path=model_path,
        db_path=db_path,
    )
    acceptance_period = cfg["first_model_params"]["acceptance_period"]
    retrain_period = cfg["first_model_params"]["retrain_period"]
    actual_model_path, test_start, test_end, train_start, train_end = get_train_test_params(
        cfg["first_model_params"],
        operation_mode,
        t0,
        acceptance_period,
        retrain_period,
    )
    logging.info("=== START phase %s ===", operation_mode)
    logging.info("database=%s", cfg["first_model_params"]["database"])
    logging.info("test_database=%s", cfg["first_model_params"]["test_database"])
    logging.info("model_path=%s", cfg["first_model_params"]["model_path"])
    avg_acc = first_model.first_model(
        actual_model_path,
        test_start,
        test_end,
        train_start,
        train_end,
        cfg["first_model_params"],
        operation_mode,
        t0,
        acceptance_period,
        retrain_period,
    )
    save_model_run(results_file, cfg, operation_mode, avg_acc)
    logging.info("=== END phase %s ===", operation_mode)
    return avg_acc
def run_test_for_each_client_on_shared_db(
    config,
    t0,
    test_mode,
    train_client_ids,
    test_client_ids,
    model_path,
    db_path,
    results_file,
):
    failures = []
    for test_client_id in ensure_list(test_client_ids):
        logging.info("=== START single-client test on shared DB: %s ===", test_client_id)
        try:
            run_phase(
                config=config,
                t0=t0,
                operation_mode=test_mode,
                train_client_ids=train_client_ids,
                test_client_ids=test_client_id,
                model_path=model_path,
                db_path=db_path,
                results_file=results_file,
            )
        except first_model.NoDataError as e:
            logging.warning("[SKIP TEST] client %s: %s", test_client_id, e)
        except Exception as e:
            logging.exception("Test failed for client %s: %s", test_client_id, e)
            failures.append(test_client_id)
        logging.info("=== END single-client test on shared DB: %s ===", test_client_id)
    return failures
def parse_args():
    parser = argparse.ArgumentParser(description="Batch runner for first_model on one shared DB")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--t0", required=True, help="Current date in YYYY-MM-DD")
    parser.add_argument("--model-path", required=True, help="Path to .pt model")
    parser.add_argument("--db-path", required=True, help="Path to shared SQLite DB")
    parser.add_argument("--results-file", default="model_runs.xlsx", help="Excel file for run results")
    parser.add_argument("--run-train", action="store_true", help="Run training phase")
    parser.add_argument("--run-test", action="store_true", help="Run test phase")
    parser.add_argument("--train-mode", default="train", help="Operation mode for training")
    parser.add_argument("--test-mode", default="test_today", help="Operation mode for testing")
    parser.add_argument("--train-client-ids", help="Single client id or comma-separated list for training")
    parser.add_argument("--test-client-ids", help="Single client id or comma-separated list for testing")
    parser.add_argument("--exclude-train-client-ids", type=str, help="Single client id or comma-separated list for training")
    parser.add_argument("--exclude-test-client-ids", type=str, help="Single client id or comma-separated list for testing")
    return parser.parse_args()
    
def main():
    args = parse_args()    
    config = load_config(args.config)
    t0 = datetime.strptime(args.t0, "%Y-%m-%d").date()
    if not args.run_train and not args.run_test:
        raise SystemExit("Podaj przynajmniej --run-train albo --run-test")
    db_path = validate_db_path(args.db_path)
    train_include = parse_client_ids(args.train_client_ids)
    test_include = parse_client_ids(args.test_client_ids)
    train_exclude = parse_client_ids(args.exclude_train_client_ids)
    test_exclude = parse_client_ids(args.exclude_test_client_ids)    
    all_clients = get_all_clients_from_db(db_path, table_name="dataset")
    resolved_train_client_ids = resolve_clients(
        all_clients=all_clients,
        include=train_include,
        exclude=train_exclude,
    )
    resolved_test_client_ids = resolve_clients(
        all_clients=all_clients,
        include=test_include,
        exclude=test_exclude,
    )
    failures = []
    model_path = args.model_path
    # TRAIN
    if args.run_train:
        try:
            effective_test_ids_for_train = resolved_test_client_ids
            run_phase(
                config=config,
                t0=t0,
                operation_mode=args.train_mode,
                train_client_ids=resolved_train_client_ids,
                test_client_ids=effective_test_ids_for_train,
                model_path=model_path,
                db_path=db_path,
                results_file=args.results_file,
            )
        except first_model.NoDataError as e:
            logging.warning("[SKIP TRAIN] %s", e)
        except Exception as e:
            logging.exception("Train failed: %s", e)
            failures.append("train")
    # TEST
    if args.run_test:
        try:
            effective_train_ids_for_test = resolved_train_client_ids
            if len(resolved_test_client_ids) > 1:
                test_failures = run_test_for_each_client_on_shared_db(
                    config=config,
                    t0=t0,
                    test_mode=args.test_mode,
                    train_client_ids=effective_train_ids_for_test,
                    test_client_ids=resolved_test_client_ids,
                    model_path=model_path,
                    db_path=db_path,
                    results_file=args.results_file,
                )
                if test_failures:
                    failures.extend([f"test:{client_id}" for client_id in test_failures])
            else:
                run_phase(
                    config=config,
                    t0=t0,
                    operation_mode=args.test_mode,
                    train_client_ids=effective_train_ids_for_test,
                    test_client_ids=resolved_test_client_ids,
                    model_path=model_path,
                    db_path=db_path,
                    results_file=args.results_file,
                )
        except first_model.NoDataError as e:
            logging.warning("[SKIP TEST] %s", e)
        except Exception as e:
            logging.exception("Test failed: %s", e)
            failures.append("test")
    if failures:
        raise SystemExit(f"Run finished with failures in phases: {failures}")
if __name__ == "__main__":
    main()
