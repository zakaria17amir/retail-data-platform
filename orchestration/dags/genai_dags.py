import os
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset
from common import START, default_args
from docker.types import Mount

ENRICHED = Asset("s3://lakehouse/silver/product_enriched")


def llm_task(task_id: str, command: list[str], **kwargs: Any) -> DockerOperator:
    """A genai/agents CLI run in the retail-agents image (the Chainlit image; genai is its path
    dependency), in the 1-slot `llm` pool: one GPU, and Ollama swaps the chat and embed models."""
    repo = os.environ["HOST_REPO_DIR"]
    db = f"postgres:5432/{os.environ.get('POSTGRES_DB', '')}"
    user = f"{os.environ.get('POSTGRES_USER', '')}:{os.environ.get('POSTGRES_PASSWORD', '')}"
    return DockerOperator(
        task_id=task_id,
        image="retail-agents",
        command=command,
        network_mode="retail_retail",
        environment={
            "LITELLM_URL": "http://litellm:4000",
            "MLFLOW_TRACKING_URI": "http://mlflow:5000",
            "MINIO_ENDPOINT": "http://minio:9000",
            "LAKEHOUSE_BUCKET": os.environ.get("LAKEHOUSE_BUCKET") or "lakehouse",
            "RECOMMEND_URL": "http://serving:8000",
            **({"ENRICH_LIMIT": v} if (v := os.environ.get("ENRICH_LIMIT")) else {}),
        },
        private_environment={
            **{
                k: os.environ.get(k, "")
                for k in ("LITELLM_MASTER_KEY", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")
            },
            "RAG_DSN": f"postgresql://{user}@{db}",
            # restricted order-writer role created by sql/shop.sql (as in the chainlit service)
            "SHOP_DSN": f"postgresql://shop_writer:shop_writer@{db}",
        },
        # the Docker socket is the host's, so bind sources must be host paths; the image ships
        # agents/src only, so the golden sets come from the checkout like gold and the dbt target.
        # sql/ (rag.sql, shop.sql) is also COPYed into the image; the mount keeps it in step
        mounts=[
            Mount(target=f"/app/{path}", source=f"{repo}/{path}", type="bind", read_only=True)
            for path in ("data/gold", "analytics/target", "agents/evals/data", "sql")
        ],
        mount_tmp_dir=False,
        mem_limit="2g",
        auto_remove="success",
        pool="llm",
        **kwargs,
    )


def genai_dag(dag_id: str, schedule: object) -> DAG:
    return DAG(
        dag_id,
        schedule=schedule,
        start_date=START,
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        tags=["genai"],
    )


# resumable: products already enriched or rejected under the same prompt hash are skipped
with genai_dag("enrich_catalogue", "@weekly"):
    llm_task("enrich", ["genai", "enrich"], outlets=[ENRICHED])

with genai_dag("rag_index", [ENRICHED]):
    llm_task("init", ["genai", "rag", "init"]) >> llm_task("index", ["genai", "rag", "index"])

with genai_dag("nightly_evals", "@daily"):
    limit = os.environ.get("EVAL_LIMIT") or "20"
    for suite in ("analytics", "shopping"):
        llm_task(suite, ["agents", "eval", suite, "--provider", "local", "--limit", limit])
