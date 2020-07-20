from __future__ import print_function
import datetime
# import pendulum
import os
# import tablib
import pathlib

from airflow import models
# from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

from google.cloud import bigquery

from galileo import galileo, searchconsole, ga

from airflow.contrib.operators import slack_webhook_operator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


def msg_slack(context):
    o = slack_webhook_operator.SlackWebhookOperator(
        task_id="tell_slack",
        http_conn_id='slack_default',
        message="{'test':'Airflow data success'}",
        channel="#observatory-tech"
        )
    return o.execute(context)


def msg_failed_slack(context):
    o = slack_webhook_operator.SlackWebhookOperator(
        task_id="tell_slack",
        http_conn_id='slack_default',
        message="{'test':'Airflow data pipeline failed'}",
        channel="#observatory-tech"
        )
    return o.execute(context)


default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 22),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5)
}

with models.DAG(
        'project_sandbox_airflow_slack',
        # schedule_interval=datetime.timedelta(days=1),
        schedule_interval='0 20 * * *',
        catchup=False,
        on_failure_callback=msg_failed_slack,
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')

    # BigQuery Scripts
    # total unique visitors 90 days snapshot
    query_unique_visitors_90days_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_unique_visitors_90days_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_sandbox/dta_sql_unique_visitors_snapshot_90days_daily_doi").read_text(),
        use_legacy_sql=False,
        on_success_callback=msg_slack
        )
      
    tell_slack = slack_webhook_operator.SlackWebhookOperator(
        task_id="tell_slack", http_conn_id='slack_default',
        message="{'test':'Data pipeline run successfully'}",
        channel="#observatory-tech",
        provide_context=True,
        on_success_callback=msg_slack)

    query_unique_visitors_90days_daily_snapshot >> tell_slack