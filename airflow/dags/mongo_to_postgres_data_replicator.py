import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    "replicate_mongo_to_postgres",
    schedule="* * * * *",
    start_date=datetime.datetime.now(),
    catchup=False,
)


def replicate_mongo_to_postgres():
    import json
    from airflow.hooks.base import BaseHook
    from pymongo import MongoClient
    import psycopg2

    mongo_conn = BaseHook.get_connection("mongo")
    mongo_client = MongoClient(
        f"mongodb://{mongo_conn.login}:{mongo_conn.password}@{mongo_conn.host}:27017"
    )
    db = mongo_client["mongo"]

    postgres_conn = BaseHook.get_connection("postgres")
    postgres_client = psycopg2.connect(
        dbname=postgres_conn.schema,
        user=postgres_conn.login,
        password=postgres_conn.password,
        host=postgres_conn.host,
    )

    with postgres_client.cursor() as postgres_cursor:
        sessions = db.UserSessions.find()
        for session in sessions:
            postgres_cursor.execute(
                """
                INSERT INTO user_sessions 
                (session_id, user_id, start_time, end_time, pages_visited, device, actions)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (session_id) DO NOTHING
            """,
                (
                    session["session_id"],
                    session["user_id"],
                    session["start_time"],
                    session["end_time"],
                    session["pages_visited"],
                    json.dumps(session["device"]),
                    session["actions"],
                ),
            )

    postgres_client.commit()
    postgres_client.close()


replicate_task = PythonOperator(
    task_id="replicate_mongo_to_postgres",
    python_callable=replicate_mongo_to_postgres,
    dag=dag,
)
