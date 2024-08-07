#!/bin/bash

set -e

. wod-iquod-qc.conf
export AWS_ACCESS_KEY_ID="$aws_access_key_id"
export AWS_SECRET_ACCESS_KEY="$aws_secret_access_key"
export AWS_REGION=us-east-1

set -x

year="$1"
dataset="$2"
check="$3"

mkdir temp
mkdir output

#spark.shuffle.service.port	7337

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
  -fs s3 \
  -ib wod-test-resources \
  -ip $date_folder/data/parquet/yearly \
  -ibr us-east-1 \
  -ia $aws_access_key_id \
  -is $aws_secret_access_key \
  -ob wod-test-resources \
  -op $date_folder/data/qc \
  -obr us-east-1 \
  -oa $aws_access_key_id \
  -os $aws_secret_access_key \
  -pb wod-test-resources \
  -pk $date_folder/resources/spark-s3.properties \
  -pbr us-east-1 \
  -pa $aws_access_key_id \
  -ps $aws_secret_access_key \
  -qc $check \
  -ds $dataset \
  -y $year \
  -gr \
  -ftc \
  -s
