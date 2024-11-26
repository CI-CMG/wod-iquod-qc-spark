#!/bin/bash

set -ex

year="$1"
dataset="$2"
check="$3"
date_folder="$4"
working_dir="$PWD"

tar -xvf OpenJDK11U-jre_x64_linux_hotspot_11.0.23_9.tar.gz
tar -xvf spark-3.5.3-bin-hadoop3-scala2.13.tgz
rm OpenJDK11U-jre_x64_linux_hotspot_11.0.23_9.tar.gz
rm spark-3.5.3-bin-hadoop3-scala2.13.tgz

mkdir -p $date_folder/data/qc/$dataset/OBS/$year
mkdir -p $date_folder/data/parquet/yearly/$dataset/OBS

tar -xvf ${dataset}${year}.parquet.tar.gz -C $date_folder/data/parquet/yearly/$dataset/OBS
rm ${dataset}${year}.parquet.tar.gz

tar -xvf resources.tar.gz
rm resources.tar.gz

if [[ $dataset = 'SUR' ]]; then
  qc_dir=$date_folder/data/qc/$dataset/OBS/SUR_ALL
else
  qc_dir=$date_folder/data/qc/$dataset/OBS/$year
fi

mkdir -p $qc_dir

shopt -s nullglob
for filename in ./*.tar.gz; do
  tar -xvf "$filename" -C $qc_dir
  rm "$filename"
done

export JAVA_HOME="$PWD/jdk-11.0.23+9-jre"
export SPARK_HOME="$PWD/spark-3.5.3-bin-hadoop3-scala2.13"
export PATH="$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH"

mkdir temp

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
  -ib "$PWD/$date_folder" \
  -ip data/parquet/yearly \
  -ob "$PWD/$date_folder" \
  -op data/qc \
  -pb resources \
  -pk spark.properties \
  -qc $check \
  -ds $dataset \
  -y $year \
  -s

cd $qc_dir
tar -czf output.tar.gz $check.parquet

cd "$working_dir"
mv $qc_dir/output.tar.gz .