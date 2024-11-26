#!/bin/bash

set -e

. wod-iquod-qc.conf

OSDF=osdf:///ospool/${access_point}/data/${username}

java -cp wod-iquod-qc-spark-${project.version}.jar:spark-3.5.3-bin-hadoop3-scala2.13/jars/* edu.colorado.cires.wod.spark.iquodqc.OsPoolUtils generate-dag -l original-wod-ascii-to-parquet-spark-list.txt -o wod-iquod-failures-json.dag -t failures -osdf $OSDF -df $date_folder
