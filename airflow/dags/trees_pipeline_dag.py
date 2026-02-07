from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
import docker

default_args = {
    "owner": "ghaith",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_producer_batch():
    """Run producer_batch.py in container using Airflow Variable for offset"""
    # Read last offset from Airflow Variable
    OFFSET = int(Variable.get("paris_trees_offset", default_var=0))

    client = docker.from_env()
    container = client.containers.get("kafka-producer")

    # Pass OFFSET as environment variable
    cmd = f"bash -c 'export OFFSET={OFFSET} && python /app/producer.py'"
    result = container.exec_run(cmd)

    output = result.output.decode()
    print(output)

    if result.exit_code != 0:
        raise Exception(f"Producer failed with exit code {result.exit_code}")

    # Extract NEXT_OFFSET from output
    for line in output.splitlines():
        if line.startswith("NEXT_OFFSET="):
            next_offset = int(line.split("=")[1])
            Variable.set("paris_trees_offset", next_offset)
            print(f"Updated Airflow Variable: paris_trees_offset = {next_offset}")
            break

def run_kafka_to_hdfs():
    client = docker.from_env()
    container = client.containers.get("spark-master")
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        "/opt/spark-apps/kafka_to_hdfs.py"
    ]
    result = container.exec_run(cmd)
    print(result.output.decode())
    if result.exit_code != 0:
        raise Exception("Kafka to HDFS job failed")

def run_hdfs_to_postgres():
    client = docker.from_env()
    container = client.containers.get("spark-master")
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--packages", "org.postgresql:postgresql:42.2.18",
        "/opt/spark-apps/hdfs_to_postgres.py"
    ]
    result = container.exec_run(cmd)
    print(result.output.decode())
    if result.exit_code != 0:
        raise Exception("HDFS to Postgres job failed")

with DAG(
    dag_id="paris_trees_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # run every 1 minute
    catchup=False,
) as dag:

    kafka_producer = PythonOperator(
        task_id="kafka_producer",
        python_callable=run_producer_batch,
    )

    kafka_to_hdfs = PythonOperator(
        task_id="kafka_to_hdfs",
        python_callable=run_kafka_to_hdfs,
    )

    hdfs_to_postgres = PythonOperator(
        task_id="hdfs_to_postgres",
        python_callable=run_hdfs_to_postgres,
    )

    kafka_producer >> kafka_to_hdfs >> hdfs_to_postgres
