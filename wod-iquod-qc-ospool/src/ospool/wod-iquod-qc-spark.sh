#!/bin/bash

set -ex

. wod-iquod-qc.conf

year="$1"
dataset="$2"
check="$3"

tar -xvf OpenJDK11U-jre_x64_linux_hotspot_11.0.23_9.tar.gz
tar -xvf spark-3.4.3-bin-hadoop3-scala2.13.tgz

export JAVA_HOME="$PWD/jdk-11.0.23+9-jre"
export SPARK_HOME="$PWD/spark-3.4.3-bin-hadoop3-scala2.13"
export PATH="$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH"

mkdir temp
mkdir output

spark-submit \
  --master 'local[4]' \
  --conf spark.cores.max=4 \
  --conf spark.ui.enabled=false \
  --conf "spark.local.dir=$(pwd)/temp" \
  --conf "spark.log.level=WARN" \
  --conf "spark.default.parallelism=2" \
  --conf "spark.port.maxRetries=100" \
  --driver-memory=19G \
  --driver-java-options '-XX:ActiveProcessorCount=4' \
  --driver-java-options "-Djava.io.tmpdir=$(pwd)/temp" \
  --class edu.colorado.cires.wod.spark.iquodqc.Sparkler \
  wod-iquod-qc-spark-${project.version}.jar \
  -ib . \
  -ip $date_folder/data/parquet/yearly \
  -ob . \
  -op $date_folder/data/qc \
  -pb . \
  -pk $date_folder/resources/spark.properties \
  -qc $check \
  -ds $dataset \
  -y $year \
  -s