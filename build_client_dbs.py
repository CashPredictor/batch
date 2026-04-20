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

def main():
    parser = argparse.ArgumentParser(description="Build per-client SQLite DBs from single Excel file")

    parser.add_argument(
        "--t0",
        required=True,
        help="Current date in YYYY-MM-DD"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml"
    )
    parser.add_argument(
        "--build-clients",
        help="Comma-separated list of client IDs to build DBs for (overrides config)"
    )

    parser.add_argument(
        "--db-path",
        required=True,
        help="Ścieżka do wspólnej bazy SQLite"
    )


    args = parser.parse_args()

    config = load_config(args.config)
    t0 = datetime.strptime(args.t0, "%Y-%m-%d").date()

    config["first_model_params"]["database"] = args.db_path
    cli_clients = parse_client_ids(args.build_clients)

    if cli_clients:
        # NADPISUJEMY tylko na potrzeby tego wywołania
        config["first_model_params"]["train_client_id"] = cli_clients
        print(f"[INFO] Using client list from CLI: {cli_clients}")
    else:
        print(
            "[INFO] Using client list from config.yaml:",
            config["first_model_params"].get("train_client_id")
        )

    # CORE: dalej zostaje EXACTLY ten sam mechanizm
    build_dataset_db.build_dataset_db(
        config["first_model_params"],
        t0,
        create_db_from_one_excel=True
    )

if __name__ == "__main__":
    main()
