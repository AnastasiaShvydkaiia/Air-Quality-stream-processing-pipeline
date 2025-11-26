from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient

MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "aq_db"
COLLECTION_NAME = "raw_collection"
DAYS_TO_KEEP = 30

def delete_old_documents():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    cutoff_date = datetime.utcnow() - timedelta(days=DAYS_TO_KEEP)
    result = collection.delete_many({"created_at": {"$lt": cutoff_date}})
    print(f"Deleted {result.deleted_count} documents older than {DAYS_TO_KEEP} days")

    client.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mongo_cleanup_dag",
    default_args=default_args,
    description="Delete old documents from MongoDB",
    schedule_interval="0 2 * * *",  
    start_date=datetime(2025, 11, 10),
    catchup=False,
) as dag:

    cleanup_task = PythonOperator(
        task_id="delete_old_docs",
        python_callable=delete_old_documents
    )

    cleanup_task
