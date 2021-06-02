from airflow import DAG
from airflow.models.dag import dag
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

METRIC_ETL_DAG_ID = 'metrics_etl_emr'


@dag(dag_id="hourly_metrics_etl_dag_run", default_args=DEFAULT_ARGS, schedule_interval="0 * * * *", tags=['hourly_metrics'])
def hourly_metrics_etl_dag_run():
    run_etl_dag = TriggerDagRunOperator(
        task_id='hourly_trigger', trigger_dag_id=METRIC_ETL_DAG_ID)

    print(run_etl_dag.output)


dag = hourly_metrics_etl_dag_run()
