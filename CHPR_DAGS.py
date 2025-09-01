# dags/CHPR_DAGS.py
from __future__ import annotations

import os
import sys
import logging
import shlex
import subprocess
from collections import deque
from datetime import datetime, timedelta
from typing import Iterable, Mapping, Any
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
try:
    # Airflow 3 style
    from airflow.sdk import Variable  # type: ignore
except Exception:  # Airflow < 3 fallback
    from airflow.models import Variable  # type: ignore

from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun

# ================================ CONFIG ====================================
EMAILS = ["nghan@chprhealth.org", "ngha.mbuh@gmail.com"]
LOCAL_TZ = pendulum.timezone("Africa/Douala")
DEFAULT_ARTIFACTS_DIR = "/tmp/airflow_artifacts"
DEFAULT_SCRIPTS_DIR = "/mnt/d/SCRIPTS_PATH"   # override with env/Variable 'script_path'

# ================================ UTILS =====================================
def _get_cfg_runtime(name: str, default: str | None = None) -> str:
    """
    Resolve config at RUNTIME with this priority:
      1) env[name] or env[name.upper()]
      2) Airflow Variable[name] or Variable[name.upper()]
      3) default (if given) else raise
    """
    env_val = os.environ.get(name) or os.environ.get(name.upper())
    if env_val:
        return env_val
    try:
        var_val = Variable.get(name, default_var=None)
        if var_val:
            return var_val
        var_val_uc = Variable.get(name.upper(), default_var=None)
        if var_val_uc:
            return var_val_uc
    except Exception:
        pass
    if default is not None:
        return default
    raise RuntimeError(f"Missing config: {name} (set env/Variable '{name}' or '{name.upper()}')")

# =============================== CALLBACKS ==================================
def on_failure_notify(context):
    log = logging.getLogger(__name__)
    ti = context.get("ti")
    dag_id = (context.get("dag_run").dag_id if context.get("dag_run")
              else context.get("dag").dag_id)
    run_id = context.get("run_id")
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown-task"
    try_number = ti.try_number if ti else "n/a"
    owner = context.get("task").owner if context.get("task") else "unknown-owner"
    log.error("❌ Failure in DAG '%s' / task '%s' (run_id=%s, try=%s, owner=%s).",
              dag_id, task_id, run_id, try_number, owner)

    message = (
        f":rotating_light: *Airflow Failure*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Run*: `{run_id}` (try {try_number})\n"
        f"*Log*: {ti.log_url if ti else 'n/a'}"
    )

    slack_conn_id = os.environ.get("SLACK_WEBHOOK_CONN_ID") or Variable.get("SLACK_WEBHOOK_CONN_ID", default_var=None)
    if slack_conn_id:
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
            SlackWebhookHook(slack_webhook_conn_id=slack_conn_id).send(text=message)
            log.info("Sent Slack alert via connection %s", slack_conn_id)
            return
        except Exception as e:
            log.warning("Slack via connection failed: %s", e)

    slack_webhook = os.environ.get("SLACK_WEBHOOK") or Variable.get("SLACK_WEBHOOK", default_var=None)
    if slack_webhook:
        try:
            import requests
            resp = requests.post(slack_webhook, json={"text": message}, timeout=10)
            if resp.ok:
                log.info("Sent Slack alert via raw webhook.")
            else:
                log.warning("Slack webhook POST failed: HTTP %s", resp.status_code)
        except Exception as e:
            log.warning("Slack via raw webhook failed: %s", e)

# =============================== DAG FACTORY =================================
def build_script_dag(
    *,
    dag_id: str,
    schedule: str | None,
    start_date: datetime,
    jobs: Iterable[Mapping],      # [{"task_id":"...","script":"rel/path.py","args":[...]}]
    edges: Iterable[tuple] = (),
    tags: list[str] | None = None,
    retries: int = 2,
    retry_delay_minutes: int = 5,
    # CONCURRENCY KNOBS (per DAG)
    max_active_runs: int | None = None,     # None => use Airflow config default
    max_active_tasks: int | None = None,    # None => use Airflow config default
    catchup: bool = False,
    retry_exponential_backoff: bool = True,
    execution_timeout_minutes: int = 60,
    dagrun_timeout_minutes: int = 180,
    sla_minutes: int | None = None,
    pool: str | None = None,
    queue: str | None = None,
    priority_weight: int | None = None,
    doc_md: str | None = None,
):
    if tags is None:
        tags = ["external-script"]

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "retry_exponential_backoff": retry_exponential_backoff,
        "email": EMAILS,
        "email_on_failure": True,
        "email_on_retry": False,
        "on_failure_callback": on_failure_notify,
    }
    if priority_weight is not None:
        default_args["priority_weight"] = priority_weight

    if not doc_md:
        job_lines = "\n".join(f"- `{j['task_id']}` → `{j['script']}`" for j in jobs)
        doc_md = f"### Pipeline `{dag_id}`\n\nJobs:\n{job_lines}\n\nSchedule: `{schedule}` (Africa/Douala)\n"

    # Build DAG decorator kwargs dynamically so older Airflow versions won’t choke
    dag_kwargs: dict[str, Any] = dict(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        tags=tags,
        default_args=default_args,
        dagrun_timeout=timedelta(minutes=dagrun_timeout_minutes),
        doc_md=doc_md,
    )
    if max_active_runs is not None:
        dag_kwargs["max_active_runs"] = max_active_runs
    if max_active_tasks is not None:
        dag_kwargs["max_active_tasks"] = max_active_tasks

    @dag(**dag_kwargs)
    def _factory_dag():
        logger = logging.getLogger(f"airflow.task.{dag_id}")

        @task(
            execution_timeout=timedelta(minutes=execution_timeout_minutes),
            sla=(timedelta(minutes=sla_minutes) if sla_minutes else None),
        )
        def run_external(
            script_rel_path: str,
            args: list[str] | None = None,
            task_instance: "TaskInstance" | None = None,   # injected by TaskFlow
            dag_run: "DagRun" | None = None,               # injected by TaskFlow
            data_interval_start=None,                      # injected by TaskFlow
        ):
            # ---- derive context (no get_current_context) ----
            task_id_local = task_instance.task_id if task_instance else "unknown"
            dag_id_local = (task_instance.dag_id if task_instance else (dag_run.dag_id if dag_run else "unknown"))
            run_id = dag_run.run_id if dag_run else "manual__unknown"
            if data_interval_start is None:
                data_interval_start = pendulum.now("UTC")

            # ---- resolve config at runtime ----
            script_base = _get_cfg_runtime("script_path", default=DEFAULT_SCRIPTS_DIR)
            artifacts_base = _get_cfg_runtime("artifacts_path", default=DEFAULT_ARTIFACTS_DIR)

            # ---- compute paths / guards ----
            script_file = Path(script_base) / script_rel_path
            logger.info("Resolved script base: %s", script_base)
            logger.info("Resolved script file: %s", script_file)

            if not script_file.exists():
                parent = script_file.parent
                sample = []
                if parent.exists():
                    try:
                        sample = [p.name for p in list(parent.glob("*"))[:40]]
                    except Exception:
                        sample = ["<error listing directory>"]
                raise AirflowException(
                    "Script not found.\n"
                    f"  Expected: {script_file}\n"
                    f"  Base dir: {script_base}\n"
                    f"  Parent exists: {parent.exists()}  |  Parent: {parent}\n"
                    f"  Parent sample: {sample}\n"
                    "Tip: Set env/Variable 'script_path' to the directory that contains your project folders."
                )

            run_dir = Path(artifacts_base) / dag_id_local / task_id_local / data_interval_start.strftime("%Y-%m-%dT%H%M%S")
            run_dir.mkdir(parents=True, exist_ok=True)

            # ---- build command ----
            safe_args = [str(a) for a in (args or [])]
            cmd = [sys.executable, str(script_file), *safe_args]
            logger.info("Launching external script: %s", " ".join(shlex.quote(c) for c in cmd))
            logger.info("Working directory for this run: %s", run_dir)

            # ---- stream logs; keep rolling tail ----
            tail = deque(maxlen=200)
            rc = None
            try:
                with subprocess.Popen(
                    cmd,
                    cwd=run_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                ) as proc:
                    assert proc.stdout is not None
                    for line in proc.stdout:
                        line = line.rstrip("\n")
                        logger.info(line)
                        tail.append(line[-1000:])
                    rc = proc.wait()
            except Exception as e:
                logger.exception("Subprocess failed to start or crashed: %s", e)
                raise

            if rc != 0:
                tail_text = "\n".join(tail)
                raise AirflowException(
                    f"Script failed (exit={rc}) for {script_file.name}\n"
                    f"Command: {' '.join(shlex.quote(c) for c in cmd)}\n"
                    f"Last output:\n{tail_text}"
                )

            return {
                "script": str(script_file),
                "args": safe_args,
                "run_dir": str(run_dir),
                "exit_code": rc,
                "tail": "\n".join(tail)[-2000:],
                "run_id": run_id,
            }

        # ---- build tasks (NO .partial) ----
        task_map: dict[str, Any] = {}
        for job in jobs:
            t_id = job["task_id"]
            script_rel = job["script"]
            job_args = job.get("args") or []

            op = run_external
            # operator-level params go in override()
            if pool or queue or (priority_weight is not None):
                kw: dict[str, Any] = {}
                if pool:
                    kw["pool"] = pool
                if queue:
                    kw["queue"] = queue
                if priority_weight is not None:
                    kw["priority_weight"] = priority_weight
                op = op.override(**kw)

            task_map[t_id] = op.override(task_id=t_id)(script_rel, job_args)

        # ---- validate / apply edges ----
        for u, v in edges:
            if u not in task_map or v not in task_map:
                known = ", ".join(sorted(task_map.keys()))
                raise ValueError(f"Invalid edge {u} -> {v}. Known tasks: [{known}]")
            task_map[u] >> task_map[v]

    return _factory_dag()

# ============================== DEFINE DAGS ==================================
# NOTE: increase per-DAG concurrency here. These values allow overlapping runs
# and multiple tasks per DAG to execute at once (subject to executor & pools).
DAG_SPECS = [

# WAVE11 USER ACTIVITY

    {
        "dag_id": "wave11_user_activity_runner",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 7, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "user_activity", "script": "WAVE11_PROJECT/WAVE11_USER_ACTIVITY.py"}],
        "edges": [],
        "tags": ["wave11", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },

# WAVE11 PIPELINE

    {
        "dag_id": "WAVE11_PIPELINE",
        "schedule": "0 6-18 * * *",  # hourly at HH:00 between 06:00–18:00
        "start_date": datetime(2025, 8, 24, 6, 15, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "wave11_data_importer",  "script": "WAVE11_PROJECT/WAVE11_DATA_IMPORTATION.py"},
            {"task_id": "wave11_data_processor", "script": "WAVE11_PROJECT/WAVE11_DATA_PROCESSING.py"},
        ],
        "edges": [("wave11_data_importer", "wave11_data_processor")],
        "tags": ["wave11", "pipeline"],
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },


# CONFIGURE FOR WAVE11 REPORT IMPORTATION


    {
        "dag_id": "WAVE11_REPORTS",
        "schedule": "5 6-18/3 * * *",                     # 06:05, 09:05, 12:05, 15:05, 18:05
        "start_date": datetime(2025, 8, 24, 6, 5, tzinfo=LOCAL_TZ),
        "catchup": False,
        "jobs": [
            {
                "task_id": "WAVE11_REPORTS_IMPORTATION",
                "script": "WAVE11_PROJECT/WAVE11_DATA_IMPORTATION_REPORTS.py",
            },

            {
                "task_id": "WAVE11_REPORTS_PROCESSING",
                "script": "WAVE11_PROJECT/WAVE11_DATA_PROCESSING.py"
            }
        ],
        "edges": [('WAVE11_REPORTS_IMPORTATION', 'WAVE11_REPORTS_PROCESSING')],
        "tags": ["wave11", "reports", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        # Keep this only if your Airflow supports per-DAG max_active_tasks; otherwise
        # set core.max_active_tasks_per_dag in airflow.cfg.
        "max_active_tasks": 16,
    },


# CONFIGURE THE GLOBAL OUTCOME USER ACTIVITY MONITORING

    {
        "dag_id": "GLOBAL_OUTCOME_USER_ACTIVITY_MONITORING",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 19, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "global_outcome_user_activity",
                  "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_USER_ACTIVITY_MONITORING.py"}],
        "edges": [],
        "tags": ["global_outcome", "user_activity", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },

# CONFIGURE THE GLOBAL OUTCOME PIPELINE

    {
        "dag_id": "GLOBAL_OUTCOME_PIPELINE",
        "schedule": "0 6-18 * * *",  # hourly
        "start_date": datetime(2025, 8, 24, 6, 17, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "global_outcome_import",
             "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_IMPORTAION.py"},
            {"task_id": "global_outcome_process",
             "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_PROCESSING.py"},
        ],
        "edges": [("global_outcome_import", "global_outcome_process")],
        "tags": ["global_outcome", "pipeline", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },


    {
        "dag_id": "GLOBAL_OUTCOME_REPORTS",
        "schedule": "5 6-18/3 * * *",                     # 06:05, 09:05, 12:05, 15:05, 18:05
        "start_date": datetime(2025, 8, 24, 6, 5, tzinfo=LOCAL_TZ),
        "catchup": False,
        "jobs": [
            {
                "task_id": "GLOBAL_OUTCOME_REPORTS_IMPORTATION",
                "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_IMPORTATION_REPORTS.py",
            },
            {
                "task_id": "global_outcome_process_reports",
                "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_PROCESSING.py"
            },
        ],
        "edges": [("GLOBAL_OUTCOME_REPORTS_IMPORTATION", "global_outcome_process_reports")],
        "tags": ["global_outcome", "reports", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        # Keep this only if your Airflow supports per-DAG max_active_tasks; otherwise
        # set core.max_active_tasks_per_dag in airflow.cfg.
        "max_active_tasks": 16,
    },


# CONFIGURE THE VIRAL LOAD USER ACTIVITY

    {
        "dag_id": "VIRAL_LOAD_USER_ACTIVITY",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 13, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "viral_load_user_activity",
                  "script": "VIRAL_LOAD_PROJECT/VIRAL_LOAD_USER_ACTIVITY.py"}],
        "edges": [],
        "tags": ["viral_load", "user_activity", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },

# CONFIGURE THE VIRAL LOAD PIPELINE

    {
        "dag_id": "VIRAL_LOAD_PIPELINE",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 18, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "viral_load_import",
             "script": "VIRAL_LOAD_PROJECT/VIRAL_LOAD_IMPORTATION_SCRIPTS.py"},
            {"task_id": "viral_load_clean",
             "script": "VIRAL_LOAD_PROJECT/VIRAL LOAD DATA CLEANING.py"},
        ],
        "edges": [("viral_load_import", "viral_load_clean")],
        "tags": ["viral_load", "pipeline", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },

# CONFIGURE VIRAL LOAD REPORT IMPORTATION

    {
        "dag_id": "VIRAL_LOAD_REPORTS",
        "schedule": "5 6-18/3 * * *",                     # 06:05, 09:05, 12:05, 15:05, 18:05
        "start_date": datetime(2025, 8, 24, 6, 5, tzinfo=LOCAL_TZ),
        "catchup": False,
        "jobs": [
            {
                "task_id": "VIRAL_LOAD_REPORT_IMPORTATION",
                "script": "VIRAL_LOAD_PROJECT/VIRAL_LOAD_IMPORTATION_SCRIPTS_REPORTS.py",
            },

            {
                "task_id": "VIRAL_LOAD_REPORT_PROCESSING",
                "script": "VIRAL_LOAD_PROJECT/VIRAL LOAD DATA CLEANING.py"
            }
        ],
        "edges": [('VIRAL_LOAD_REPORT_IMPORTATION', 'VIRAL_LOAD_REPORT_PROCESSING')],
        "tags": ["viral_load", "reports", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        # Keep this only if your Airflow supports per-DAG max_active_tasks; otherwise
        # set core.max_active_tasks_per_dag in airflow.cfg.
        "max_active_tasks": 16,
    },


# CONFIGURE THE XPERT PROCESSING

    {
        "dag_id": "XPERT_PROCESSING",
        "schedule": "0 6-18 * * *",  # hourly
        "start_date": datetime(2025, 8, 24, 6, 22, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "XPERT_DATA_PROCESSING", 
                  "script": "XPERT_PROJECT/XPERT_PROCESSING.py"}],
        "edges": [],
        "tags": ["xpert", "pipeline", "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },

# CONFIGURE THE LAB PDF PIPELINE

    {
        "dag_id": "LAB_PDF_PIPELINE",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 25, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "LAB_PDF_IMPORTATION",
             "script": "PDF_PROJECT/PDF GENERATION DATA IMPORTATION.py"},
            {"task_id": "LAB_PDF_CLEANING",
             "script": "PDF_PROJECT/PDF_GENERATION_PROCESSING_ONLY.py"},
        ],
        "edges": [("LAB_PDF_IMPORTATION", "LAB_PDF_CLEANING")],
        "tags": ["Lab PDF generation", "pipeline", "external-script", "Importation", "Cleaning"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },

# CONFIGURE A DAG TO CLEAN UP THE WORKING SPACE
    

        {
        "dag_id": "WORKING_DIRECTORY_CLEANUP",
        "schedule": "15 6-18 * * *",  # hourly
        "start_date": datetime(2025, 8, 24, 6, 22, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "WORKING_DIRECTORY_CLEANUP", 
                  "script": "delete_redcap_log_files.py"}],
        "edges": [],
        "tags": ["Cleanup Task",  "external-script"],
        "retries": 5,
        "retry_delay_minutes": 5,
        "max_active_runs": 8,
        "max_active_tasks": 16,
    },


]

# ============================= REGISTER DAGS ================================
for spec in DAG_SPECS:
    dag_obj = build_script_dag(**spec)
    globals()[spec["dag_id"]] = dag_obj




