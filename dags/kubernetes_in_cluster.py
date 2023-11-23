from datetime import datetime

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

gke_conn_id: str = "external_kubernetes_cluster"

# https://docs.astronomer.io/astro/kubernetespodoperator

namespace = conf.get("kubernetes", "NAMESPACE")


@dag(
    dag_id="kubernetes_in_cluster",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
)
def kubernetes_in_cluster():
    @task.kubernetes(
        namespace=namespace,
        image="python:3.10-slim",
        name="print_hello_world",
        get_logs=True,
        log_events_on_failure=True,
        do_xcom_push=True,
    )
    def print_hello_world():
        # Logic to write "hello world" to a file in the persistent volume
        print("Hello World")

    run_pod = KubernetesPodOperator(
        namespace=namespace,
        image="python:3.10-slim",  # Specify the Docker image to use
        cmds=["sh", "-c"],
        arguments=["echo 'hello world'"],
        name="run_bash_command",
        task_id="run_bash_command",
        get_logs=True,
    )

    hello_world = print_hello_world()
    hello_world >> run_pod


kubernetes_in_cluster()
