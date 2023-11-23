import pendulum
from airflow.decorators import dag, task
from kubernetes.client import models as k8s

from observatory.platform.gcp import gcp_delete_disk, gcp_create_disk
from observatory.platform.gke import gke_create_volume, gke_delete_volume


def make_dag(
    *,
    dag_id: str,
    project_id: str,
    zone: str,
    volume_name: str = "data-volume",
    volume_size: int = 10,
    volume_path: str = "/data",
    kubernetes_conn_id: str = "gke_cluster",
    start_date: pendulum.DateTime = pendulum.DateTime(2023, 1, 1),
    schedule_interval: str = None,
    catchup: bool = False,
    queue: str = "default",
    retries: int = 3,
    max_active_runs: int = 1,
    on_failure_callback=None
):

    # doc_md=self.__doc__,
    @dag(
        dag_id=dag_id,
        start_date=start_date,
        schedule_interval=schedule_interval,
        catchup=catchup,
        max_active_runs=max_active_runs,
        tags=["example"],
        default_args={
            "owner": "airflow",
            "on_failure_callback": on_failure_callback,
            "retries": retries,
            "queue": queue,
        },
    )
    def kubernetes_pod_storage():
        @task
        def create_pod_storage(**context):
            gcp_create_disk(project_id=project_id, zone=zone, disk_name=volume_name, disk_size_gb=volume_size)
            gke_create_volume(kubernetes_conn_id=kubernetes_conn_id, volume_name=volume_name, size_gi=volume_size)

        @task.kubernetes(
            image="python:3.10-slim",
            kubernetes_conn_id=kubernetes_conn_id,
            in_cluster=False,
            namespace="coki-astro",
            name="task_kubernetes_decorator",
            get_logs=True,
            log_events_on_failure=True,
            do_xcom_push=True,
            startup_timeout_seconds=300,
            volumes=[
                k8s.V1Volume(
                    name=volume_name,
                    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=volume_name),
                )
            ],
            volume_mounts=[k8s.V1VolumeMount(mount_path=volume_path, name=volume_name)],
        )
        def task_kubernetes_decorator(**context):
            import subprocess

            # Logic to write "hello world" to a file in the persistent volume
            print("Hello World")

            # Run the command
            result = subprocess.run(["df", "-h"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            # Get the standard output
            output = result.stdout

            # Print the output
            print(output)

            # Optionally, handle errors
            if result.returncode != 0:
                print("Error:", result.stderr)

        @task
        def delete_pod_storage(**context):
            # Delete volume and disks
            gke_delete_volume(kubernetes_conn_id=kubernetes_conn_id, volume_name=volume_name)
            gcp_delete_disk(project_id=project_id, zone=zone, disk_name=volume_name)

        t1 = create_pod_storage()
        t2 = task_kubernetes_decorator()
        t3 = delete_pod_storage()
        t1 >> t2 >> t3

    return kubernetes_pod_storage()


make_dag(dag_id="kubernetes_pod_storage", project_id="coki-jamie-dev", zone="us-central1-a")
