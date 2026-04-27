import requests
import time
import sys


class DatabricksAPI:
    def __init__(self, host, client_id, client_secret, email):
        self.host = host.rstrip("/")
        # self.host = host

        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self._get_oauth_token()
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.email = email
        self.clusters = self.ClusterManager(self)
        self.jobs = self.JobManager(self)

    def _get_oauth_token(self):
        url = f"{self.host}/oidc/v1/token"
        data = {"grant_type": "client_credentials", "scope": "all-apis"}
        resp = requests.post(url, data=data, auth=(self.client_id, self.client_secret))
        resp.raise_for_status()
        return resp.json().get("access_token")

    class ClusterManager:
        def __init__(self, outer):
            self.outer = outer

        def create(self, name):
            get_url = f"{self.outer.host}/api/2.0/clusters/list"
            get_resp = requests.get(get_url, headers=self.outer.headers)
            cluster_id = None
            for cluster in get_resp.json().get("clusters", []):
                if cluster.get("cluster_name") == name:
                    print(f"Cluster {name} already exists.")
                    cluster_id = cluster.get("cluster_id")
                    return cluster_id

            url = f"{self.outer.host}/api/2.0/clusters/create"
            payload = {
                "cluster_name": name,
                "spark_version": "17.3.x-scala2.13",
                "node_type_id": "Standard_F4",
                "autotermination_minutes": 15,
                "num_workers": 0,
                "spark_conf": {
                    "spark.master": "local[*]"
                },
                "custom_tags": {
                    "ResourceClass": "singleNode",
                    "source": "api",
                },
                "data_security_mode": "SINGLE_USER",
                "single_user_name": self.outer.client_id,
                "runtime_engine": "STANDARD"
            }
            resp = requests.post(url, json=payload, headers=self.outer.headers)
            print(f"Status: {resp.status_code}")
            print(f"Response: {resp.text}")
            resp.raise_for_status()
            cluster_id = resp.json().get("cluster_id")
            print(f"Cluster created: {cluster_id}")
            return cluster_id

        def terminate(self, cluster_id):
            url = f"{self.outer.host}/api/2.1/clusters/forced-delete"
            requests.post(url, json={"cluster_id": cluster_id}, headers=self.outer.headers)
            print(f"Cluster {cluster_id} terminated.")

        def delete(self, cluster_id):
            url = f"{self.outer.host}/api/2.0/clusters/permanent-delete"
            requests.post(url, json={"cluster_id": cluster_id}, headers=self.outer.headers)
            print(f"Cluster {cluster_id} permanently deleted.")

        def start(self, cluster_id):
            url = f"{self.outer.host}/api/2.0/clusters/start"
            requests.post(url, json={"cluster_id": cluster_id}, headers=self.outer.headers)
            print(f"Cluster {cluster_id} started.")

    class JobManager:
        def __init__(self, outer):
            self.outer = outer

        def create(self, job_name, notebook_path, cluster_id):
            url = f"{self.outer.host}/api/2.0/jobs/create"
            jobs_url = f"{self.outer.host}/api/2.0/jobs/list"
            for job in requests.get(jobs_url, headers=self.outer.headers).json().get("jobs", []):
                if job.get("settings", {}).get("name") == job_name:
                    print(f"Job {job_name} already exists.")
                    return job.get("job_id")

            payload = {
                "name": job_name,
                "tasks": [{
                    "task_key": "main_task",
                    "existing_cluster_id": cluster_id,
                    "notebook_task": {"notebook_path": notebook_path}
                }]
            }
            resp = requests.post(url, json=payload, headers=self.outer.headers)
            return resp.json().get("job_id")

        def run_now(self, job_id):
            url = f"{self.outer.host}/api/2.0/jobs/run-now"
            resp = requests.post(url, json={"job_id": job_id}, headers=self.outer.headers)
            return resp.json().get("run_id")

        def cancel_run(self, run_id):
            url = f"{self.outer.host}/api/2.1/jobs/runs/cancel"
            requests.post(url, json={"run_id": run_id}, headers=self.outer.headers)
            print(f"Run {run_id} cancelled.")

        def list_runs(self, job_id):
            url = f"{self.outer.host}/api/2.1/jobs/runs/list"
            resp = requests.get(url, params={"job_id": job_id, "active_only": "true"}, headers=self.outer.headers)
            return resp.json().get("runs", [])

        def delete(self, job_id):
            url = f"{self.outer.host}/api/2.1/jobs/delete"
            requests.post(url, json={"job_id": job_id}, headers=self.outer.headers)
            print(f"Job {job_id} deleted.")

        def monitor_run(self, run_id, poll_interval=20):
            url = f"{self.outer.host}/api/2.1/jobs/runs/get"

            print(f"Monitoring Run ID: {run_id}")
            while True:
                resp = requests.get(url, params={"run_id": run_id}, headers=self.outer.headers)
                resp.raise_for_status()
                data = resp.json()

                state = data.get("state", {})
                life_cycle = state.get("life_cycle_state")
                result_state = state.get("result_state", "N/A")

                if life_cycle in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                    print(f"\nRun Finished. Final State: {result_state}")
                    return result_state

                if life_cycle == "BLOCKED":
                    print("Run is BLOCKED. Check cluster resources.")
                else:
                    print(f"Status: {life_cycle}...", end="\r")

                time.sleep(poll_interval)

        def get_run_status(self, run_id):
            url = f"{self.outer.host}/api/2.1/jobs/runs/get"
            resp = requests.get(url, params={"run_id": run_id}, headers=self.outer.headers)
            return resp.json().get("state", {})

    def wait_for_cluster(self, cluster_id):
        url = f"{self.host}/api/2.1/clusters/get"
        while True:
            resp = requests.get(url, params={"cluster_id": cluster_id}, headers=self.headers).json()
            state = resp.get("state")
            if state == "RUNNING":
                break
            if state == "ERROR":
                raise Exception(f"Cluster failed to start: {state}")
            if state == "TERMINATED":
                self.clusters.start(cluster_id)
            print(f"Waiting for cluster {cluster_id} to be READY... ({state})")
            time.sleep(20)