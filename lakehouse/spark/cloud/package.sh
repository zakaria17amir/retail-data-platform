#!/bin/sh
# Builds the EMR Serverless job bundle (no make needed):
#   dist/lakehouse_spark.zip  - the lakehouse_spark package, for --py-files
#   dist/entrypoint.py        - the spark-submit entry point (lakehouse_spark/cloud/entrypoint.py)
# Delta is not bundled: on EMR use the release's built-in Delta (spark.jars=
# /usr/share/aws/delta/lib/delta-spark.jar,/usr/share/aws/delta/lib/delta-storage.jar), elsewhere
# --packages io.delta:delta-spark_2.13:<version>.
# Usage: cloud/package.sh [ZIP_PATH]   (PYTHON overrides the interpreter, default python3)
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
out=${1:-$here/dist/lakehouse_spark.zip}
mkdir -p "$(dirname "$out")"
"${PYTHON:-python3}" - "$here/src" "$out" <<'EOF'
import pathlib
import sys
import zipfile

src, out = pathlib.Path(sys.argv[1]), sys.argv[2]
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as bundle:
    for path in sorted((src / "lakehouse_spark").rglob("*.py")):
        bundle.write(path, path.relative_to(src).as_posix())
EOF
cp "$here/src/lakehouse_spark/cloud/entrypoint.py" "$(dirname "$out")/entrypoint.py"
echo "built $out"
