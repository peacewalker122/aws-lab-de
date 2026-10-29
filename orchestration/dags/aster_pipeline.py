from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow import DAG

RUN_ID = "{{ run_id | replace('.', '-') | replace(':', '-') | replace('+', '-') }}"


def aster(command):
    return f"python -m aster --run-id '{RUN_ID}' {command}"


def gold_command():
    return (
        'START="{{ params.window_start }}"; END="{{ params.window_end }}"; '
        f'if [ -n "$START" ] && [ -n "$END" ]; then {aster("build-gold")} '
        '--start "$START" --end "$END"; else '
        f'{aster("build-gold")}; fi'
    )


with DAG(
    dag_id="aster_pipeline",
    description="Aster ingestion, validation, transformation and publication",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "aster",
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
    params={"window_start": "", "window_end": ""},
    tags=["aster"],
    doc_md=(
        "Runs the complete Aster pipeline. Set `window_start` and `window_end` "
        "(YYYY-MM-DD) to build a windowed gold dataset for backfill; the windowed "
        "result is published under artifacts and is not promoted to the committed "
        "gold pointer."
    ),
) as dag:
    extract_sources = BashOperator(
        task_id="extract_sources",
        bash_command=aster("extract"),
        append_env=True,
    )
    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=aster("validate-raw"),
        append_env=True,
    )
    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=aster("transform-silver"),
        append_env=True,
    )
    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=aster("validate-silver"),
        append_env=True,
    )
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=gold_command(),
        append_env=True,
    )
    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=aster("validate-gold"),
        append_env=True,
    )
    publish_run = BashOperator(
        task_id="publish_run",
        bash_command=aster("publish-run"),
        append_env=True,
    )

    (
        extract_sources
        >> validate_raw
        >> transform_silver
        >> validate_silver
        >> build_gold
        >> validate_gold
        >> publish_run
    )
