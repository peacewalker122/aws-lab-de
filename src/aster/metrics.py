from pathlib import Path

from aster import lake
from aster.transform import connect as duckdb_connect, sql_list

ANALYTICS_SQL = Path(__file__).resolve().parents[2] / "sql/analytics"
METRICS = tuple(sorted(path.stem for path in ANALYTICS_SQL.glob("*.sql")))


def warehouse_connection(config):
    return duckdb_connect(config)


def create_gold_views(connection, s3, config, pointer=None):
    pointer = pointer or lake.committed_pointer(s3, config.gold_bucket, "gold")
    if pointer is None:
        raise ValueError("No committed gold dataset is available")
    for table, info in pointer["tables"].items():
        uris = [lake.s3_uri(config.gold_bucket, key) for key in info["objects"]]
        connection.execute(
            f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet([{sql_list(uris)}], union_by_name=true)"
        )
    return pointer


def run_metric(connection, name):
    if name not in METRICS:
        raise ValueError(f"Unknown metric {name}; expected one of {', '.join(METRICS)}")
    result = connection.execute((ANALYTICS_SQL / f"{name}.sql").read_text())
    columns = [column[0] for column in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def run_all(connection):
    return {name: run_metric(connection, name) for name in METRICS}
