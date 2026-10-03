#!/bin/sh
set -eu
umask 077
{
  cat /opt/spark-defaults.conf.template
  echo "spark.hadoop.fs.s3a.access.key ${MINIO_ROOT_USER:?}"
  echo "spark.hadoop.fs.s3a.secret.key ${MINIO_ROOT_PASSWORD:?}"
} > "$SPARK_HOME/conf/spark-defaults.conf"
exec "$@"
