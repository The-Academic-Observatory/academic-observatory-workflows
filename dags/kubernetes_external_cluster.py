from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)

KUBERNETES_CONN_ID: str = "gke_cluster"
NAMESPACE = "coki-astro"


@dag(
    dag_id="kubernetes_external_cluster",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
)
def kubernetes_external_cluster():

    gke_start_task = GKEStartPodOperator(
        image="alpine",
        in_cluster=False,
        namespace=NAMESPACE,
        project_id="coki-jamie-dev",
        location="us-central1",
        cluster_name="coki-dev-cluster",
        do_xcom_push=True,
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        task_id="gke_start_task",
        name="gke_start_task",
        on_finish_action="delete_pod",
    )

    @task.kubernetes(
        image="python:3.10-slim",
        kubernetes_conn_id=KUBERNETES_CONN_ID,
        in_cluster=False,
        namespace=NAMESPACE,
        name="task_kubernetes_decorator",
        get_logs=True,
        log_events_on_failure=True,
        do_xcom_push=True,
    )
    def task_kubernetes_decorator():
        # Logic to write "hello world" to a file in the persistent volume
        print("Hello World")

    kubernetes_pod_op = KubernetesPodOperator(
        image="python:3.10-slim",  # Specify the Docker image to use
        kubernetes_conn_id=KUBERNETES_CONN_ID,
        in_cluster=False,
        namespace=NAMESPACE,
        cmds=["sh", "-c"],
        arguments=["echo 'hello world'"],
        name="kubernetes_pod_op",
        task_id="kubernetes_pod_op",
        get_logs=True,
    )

    task_kubernetes_decorator_task = task_kubernetes_decorator()
    gke_start_task >> task_kubernetes_decorator_task >> kubernetes_pod_op


kubernetes_external_cluster()


# https://docs.astronomer.io/astro/kubernetespodoperator
# https://stackoverflow.com/questions/72379080/configure-volumes-in-airflow-gkestartpodoperator-operator
# https://cloud.google.com/composer/docs/composer-2/use-gke-operator
# https://cloud.google.com/composer/docs/composer-2/use-kubernetes-pod-operator
# [2023-11-20, 01:15:10 UTC] {credentials_provider.py:353} INFO - Getting connection using `google.auth.default()` since no explicit credentials are provided.

# @task
# def echo_creds():
#     print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
