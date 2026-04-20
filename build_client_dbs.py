# build_client_db.py
import argparse
from datetime import datetime
import yaml
import build_dataset_db

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def parse_client_ids(raw):
    if raw is None:
        return None
    return [int(x.strip()) for x in raw.split(",") if x.strip()]

def ensure_db_initialized(config_path: str, db_path: str):
    if os.path.exists(db_path):
        print(f"[INFO] DB already exists, skipping init: {db_path}")
        return
    print(f"[INFO] DB does not exist, initializing: {db_path}")
    result = subprocess.run(
        [
            sys.executable,
            "db_init.py",
            "--config", config_path,
            "--db-path", db_path,
        ],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"db_init.py failed with exit code {result.returncode}")    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--t0", required=True)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--include-clients")
    parser.add_argument("--exclude-clients")
    args = parser.parse_args()
    config = load_config(args.config)
    t0 = datetime.strptime(args.t0, "%Y-%m-%d").date()
    ensure_db_initialized(args.config, args.db_path)    
    config["first_model_params"]["database"] = args.db_path
    include_clients = parse_client_ids(args.include_clients)
    exclude_clients = parse_client_ids(args.exclude_clients)
    print(f"[INFO] include={include_clients}, exclude={exclude_clients}")
    build_dataset_db.build_dataset_db(
        config["first_model_params"],
        t0,
        include_clients=include_clients,
        exclude_clients=exclude_clients
    )

if __name__ == "__main__":
    main()
