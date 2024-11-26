#!/bin/bash

set -e
. wod-iquod-qc.conf

OSPOOL=/ospool/${access_point}/data/$username
OSDF=osdf://${OSPOOL}
QC_FOLDER=${OSPOOL}/$date_folder/data/qc

mkdir -p "${QC_FOLDER}"

java -cp wod-iquod-qc-spark-${project.version}.jar:spark-3.5.3-bin-hadoop3-scala2.13/jars/* edu.colorado.cires.wod.spark.iquodqc.OsPoolUtils generate-dag  -l original-wod-ascii-to-parquet-spark-list.txt -o wod-iquod-qc.dag -osdf $OSDF -df $date_folder -pd $QC_FOLDER
