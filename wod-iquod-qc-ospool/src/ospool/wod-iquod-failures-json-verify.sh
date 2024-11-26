#!/bin/bash

set -ex

. wod-iquod-qc.conf
QC=/ospool/${access_point}/data/${username}/${date_folder}/data/json
home_dir="$PWD"
while read line; do
  wod_line="$line"
  IFS=',' read -r year dataset <<< ${line}
  echo ${dataset}
  echo ${year}
  OUTPUT=${dataset}/OBS/${year}/failure.json.tar.gz
  if [ -f "${QC}/${OUTPUT}" ]; then
          echo "${QC}/$OUTPUT found"

   else
           echo "$OUTPUT does not exist."
           echo "$wod_line" >> ${home_dir}/failed-original-wod-ascii-to-parquet-spark-list.txt
   fi

done < ${home_dir}/original-wod-ascii-to-parquet-spark-list.txt

cd ${home_dir}