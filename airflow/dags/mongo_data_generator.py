import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="generate_mongo_data",
    schedule="* * * * *",
    start_date=datetime.datetime.now(),
    catchup=False,
)


def generate_data():
    from airflow.hooks.base import BaseHook
    from pymongo import MongoClient
    from faker import Faker

    mongo_conn = BaseHook.get_connection("mongo")
    mongo_client = MongoClient(
        f"mongodb://{mongo_conn.login}:{mongo_conn.password}@{mongo_conn.host}:27017"
    )

    db = mongo_client["mongo"]

    fake = Faker("ru_RU")

    db.UserSessions.insert_one(
        {
            "session_id": fake.uuid4(),
            "user_id": fake.uuid4(),
            "start_time": fake.date_time_this_month(),
            "end_time": fake.date_time_this_month(),
            "pages_visited": [fake.uri() for _ in range(10)],
            "device": fake.user_agent(),
            "actions": [fake.word() for _ in range(10)],
        }
    )


generate_task = PythonOperator(
    task_id="generate_data", python_callable=generate_data, dag=dag
)

generate_task
