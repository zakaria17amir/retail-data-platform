# the official image ships neither a Postgres driver nor boto3
FROM ghcr.io/mlflow/mlflow:v3.16.1@sha256:06058cc872276873e9759c635e773342aca4731e240f6d20d88ea43774083984

RUN pip install --no-cache-dir psycopg2-binary==2.9.13 boto3==1.43.103 botocore==1.43.103 s3transfer==0.19.2 jmespath==1.1.0
