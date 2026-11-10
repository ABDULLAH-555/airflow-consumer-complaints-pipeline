from datetime import datetime

from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

from etl import extract, load, transform

with DAG(
    dag_id="consumer_financial_etl",
    description="Airflow Orchestration for Consumer Financial Complaints",
    schedule="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["Assignment"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_from_source",
        python_callable=extract.extract_all,
    )

    dump_mysql_task = PythonOperator(
        task_id="dump_data_to_mysql",
        python_callable=load.dump_to_mysql,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform.transform_data,
    )

    check_file_task = FileSensor(
        task_id="check_consumer_financial_csv",
        filepath="/opt/airflow/dags/consumer_complaints_transformed.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 30,
        mode="reschedule",
    )

    load_task = PythonOperator(
        task_id="dump_googlesheet",
        python_callable=load.dump_to_googlesheet,
    )

    send_email_task = EmailOperator(
        task_id="send_googlesheet_url_via_email",
        to="turkkhalid0@gmail.com",
        subject="Consumer financial ETL complete — {{ ds }}",
        html_content=(
            "<p>The daily consumer complaints report is ready:</p>"
            "<p><a href='{{ ti.xcom_pull(task_ids=\"dump_googlesheet\", "
            "key=\"sheet_url\") }}'>Open the Google Sheet</a></p>"
        ),
        conn_id="smtp_default",
    )

    extract_task >> dump_mysql_task >> transform_task >> check_file_task >> load_task >> send_email_task
