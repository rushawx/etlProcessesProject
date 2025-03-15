import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    "create_postgres_data_mart",
    schedule="* * * * *",
    start_date=datetime.datetime.now(),
    catchup=False,
)


def create_postgres_data_mart():
    from airflow.hooks.base import BaseHook
    import psycopg2

    postgres_conn = BaseHook.get_connection("postgres")
    postgres_client = psycopg2.connect(
        dbname=postgres_conn.schema,
        user=postgres_conn.login,
        password=postgres_conn.password,
        host=postgres_conn.host,
    )

    with postgres_client.cursor() as postgres_cursor:
        postgres_cursor.execute(
            """
            CREATE OR REPLACE VIEW dau AS
            SELECT
                date_trunc('day', start_time) as dt,
                count(distinct user_id) as dau
            FROM public.user_sessions
            GROUP BY dt
            ORDER BY dt desc
        """
        )

    with postgres_client.cursor() as postgres_cursor:
        postgres_cursor.execute(
            """
            CREATE OR REPLACE VIEW mau AS
            SELECT
                date_trunc('month', start_time) as dt,
                count(distinct user_id) as mau
            FROM public.user_sessions
            GROUP BY dt
            ORDER BY dt desc
        """
        )

    postgres_client.commit()
    postgres_client.close()


create_task = PythonOperator(
    task_id="create_postgres_data_mart",
    python_callable=create_postgres_data_mart,
    dag=dag,
)
