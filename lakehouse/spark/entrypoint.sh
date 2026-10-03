#!/bin/sh
set -eu
{
  cat /opt/spark-defaults.conf.template
  echo "spark.hadoop.fs.s3a.access.key ${MINIO_ROOT_USER:-minio}"
  echo "spark.hadoop.fs.s3a.secret.key ${MINIO_ROOT_PASSWORD:-minio12345}"
} > "$SPARK_HOME/conf/spark-defaults.conf"
exec "$@"
