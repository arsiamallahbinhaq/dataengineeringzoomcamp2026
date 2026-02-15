import duckdb
import requests
from pathlib import Path

# ===============================
# CONFIG
# ===============================
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
TAXI_TYPE = "fhv"
YEAR = 2019
DUCKDB_FILE = "taxi_rides_ny.duckdb"
# ===============================


def download_and_convert_files():
    data_dir = Path("data") / TAXI_TYPE
    data_dir.mkdir(exist_ok=True, parents=True)

    for month in range(1, 13):
        parquet_filename = f"{TAXI_TYPE}_tripdata_{YEAR}-{month:02d}.parquet"
        parquet_filepath = data_dir / parquet_filename

        if parquet_filepath.exists():
            print(f"Skipping {parquet_filename} (already exists)")
            continue

        csv_gz_filename = f"{TAXI_TYPE}_tripdata_{YEAR}-{month:02d}.csv.gz"
        csv_gz_filepath = data_dir / csv_gz_filename

        print(f"Downloading {csv_gz_filename}...")

        response = requests.get(
            f"{BASE_URL}/{csv_gz_filename}",
            stream=True
        )
        response.raise_for_status()

        with open(csv_gz_filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Converting {csv_gz_filename} to Parquet...")

        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_csv_auto('{csv_gz_filepath}')
            )
            TO '{parquet_filepath}' (FORMAT PARQUET);
        """)
        con.close()

        csv_gz_filepath.unlink()
        print(f"Completed {parquet_filename}")


def update_gitignore():
    gitignore_path = Path(".gitignore")
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    if "data/" not in content:
        with open(gitignore_path, "a") as f:
            f.write("\n# Data directory\ndata/\n")


def load_to_duckdb():
    print("Loading Parquet files into DuckDB...")

    con = duckdb.connect(DUCKDB_FILE)

    # Use raw schema (best practice for dbt source)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    con.execute(f"""
        CREATE OR REPLACE TABLE raw.fhv_tripdata AS
        SELECT *
        FROM read_parquet('data/{TAXI_TYPE}/*.parquet', union_by_name=true)
    """)

    total = con.execute("""
        SELECT COUNT(*) FROM raw.fhv_tripdata
    """).fetchone()[0]

    print(f"Total rows loaded: {total:,}")

    con.close()


if __name__ == "__main__":
    update_gitignore()
    download_and_convert_files()
    load_to_duckdb()

    print("====================================")
    print("FHV 2019 successfully loaded!")
    print("You can now run: dbt build --select stg_fhv_tripdata --target prod")
    print("====================================")
