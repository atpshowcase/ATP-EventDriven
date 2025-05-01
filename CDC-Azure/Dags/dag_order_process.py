from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Fungsi yang akan dijalankan oleh task
def order_task():
    print("Processing order...")

# Cek apakah DAG sudah terdefinisi
dag_id = "dag_order_process"
if dag_id not in globals():
    # Mendefinisikan DAG jika belum ada
    with DAG(
        dag_id=dag_id,  # ID DAG
        start_date=datetime(2023, 5, 1),
        schedule_interval=None,
        catchup=False
    ) as dag:
        t1 = PythonOperator(
            task_id="process_order",
            python_callable=order_task
        )
else:
    print(f"DAG {dag_id} sudah ada. Tidak perlu dibuat ulang.")
