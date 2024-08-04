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
  --driver-memory=19G \
  --driver-java-options '-XX:ActiveProcessorCount=4' \
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
  -ftc
