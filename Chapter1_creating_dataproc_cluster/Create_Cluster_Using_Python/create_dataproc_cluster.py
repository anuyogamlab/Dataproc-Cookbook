import os

from google.cloud import dataproc

def create_dataproc_cluster(project_id, region, cluster_name):
    """Creates a Dataproc cluster."""
    client = dataproc.DataprocClient()

    cluster_config = dataproc.ClusterConfig()
    cluster_config.master_machine_type = "n1-standard-4"
    cluster_config.worker_machine_type = "n1-standard-4"
    cluster_config.num_workers = 2

    cluster = client.create_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        cluster_config=cluster_config,
    )

    print(f"Created Dataproc cluster: {cluster.name}")


if __name__ == "__main__":
    project_id = os.environ["PROJECT_ID"]
    region = os.environ["REGION"]
    cluster_name = "my-cluster"

    create_dataproc_cluster(project_id, region, cluster_name)