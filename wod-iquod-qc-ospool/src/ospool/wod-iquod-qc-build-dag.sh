#!/bin/bash

set -e


java -cp wod-iquod-qc-spark-${project.version}.jar:spark-3.4.3-bin-hadoop3-scala2.13/jars/* edu.colorado.cires.wod.spark.iquodqc.OsPoolUtils generate-dag -l original-wod-ascii-to-parquet-spark-list.txt -o wod-iquod-qc.dag
