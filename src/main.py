from databricks_api_manager import *
from dotenv import load_dotenv
import os

load_dotenv()
import os

host = os.getenv("DATABRICKS_HOST")
if not host:
    print("ERROR: DATABRICKS_HOST environment variable is not set!")
else:
    print(f"DATABRICKS_HOST is set (Length: {len(host)})")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
email = os.getenv("EMAIL")
pipeline_id = "35dc93cc-05e2-41a3-a00d-9a291af3a9fd"

api = DatabricksAPI(host, client_id, client_secret, email)
cluster_id = None
job_id = None

try:
    # Start a cluster
    cluster_id = api.clusters.create("tokariev_api_cluster")
    api.wait_for_cluster(cluster_id)

    # Create a new job
    job_id = api.jobs.create("test_etl", f"/Workspace/Users/{email}/temp_notebooks/test_notebook", cluster_id)

    # Start a run and monitor it
    run_id = api.jobs.run_now(job_id)
    final_status = api.jobs.monitor_run(run_id)

    if final_status == "SUCCESS":
        print("Job completed successfully.")
    else:
        print(f"Job failed with status: {final_status}")

    # Run a pipeline
    update_id = api.pipelines.start_update(pipeline_id)
    api.pipelines.monitor_update(pipeline_id, update_id)

# Cleanup to avoid residual resources
finally:
    pass
    # if job_id:
    #     api.jobs.delete(job_id)
    if cluster_id:
        api.clusters.terminate(cluster_id)
