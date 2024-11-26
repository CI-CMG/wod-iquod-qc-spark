# NOAA / NCEI World Ocean Database IQUOD QC Job

## Introduction

This project defines an Apache Spark Job that will perform IQUOD QC on World Ocean 
Database files in the Parquet format defined by https://github.com/CI-CMG/wod-parquet-model.

## Usage

### OSPool

#### Initial setup (On the OSPool gateway)

This job requires some resources to be present on the OSPool access point.  These will be copied to the workers
when the job is submitted.  Run the following if these files do not exist.

```bash
wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.23%2B9/OpenJDK11U-jre_x64_linux_hotspot_11.0.23_9.tar.gz
wget https://downloads.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3-scala2.13.tgz
tar -xvf spark-3.5.3-bin-hadoop3-scala2.13.tgz
```

#### On your laptop

Copy the zip file to your OSPool gateway (assuming you have an ospool SSH alias)
```bash
scp wod-iquod-qc-ospool-1.2.0.zip ospool:~/
```

SSH into the OSPool gateway (assuming you have an ospool SSH alias)
```bash
ssh ospool
```

Test resources need to be set up in the S3 bucket.  Copy these and spark-s3.properties to $date_folder/resources in the bucket.

#### Submit the job (On the OSPool gateway)

Unzip the bundle
```bash
unzip wod-iquod-qc-ospool-1.2.0.zip
```
Edit the wod-iquod-qc.conf file and set the OSPOOL username, access point, and date folder.
Note: can not contain spaces
```bash
vim wod-iquod-qc.conf
```

Edit the wod-iquod-qc-spark.submit and set the OSPOOL username and access_point
```bash
username =
access_point =
```

Build DAG file.  This uses the file created from verifying the conversion to parquet (original-wod-ascii-to-parquet-spark-list.txt)
```bash
./wod-iquod-qc-build-dag.sh
```

Clean up any existing logs and DAG state
```bash
rm -rf wod-iquod-qc-spark
rm wod-iquod-qc.dag.*
```

Execute the job. 
The optimal value needs to be determined with a max of 10000 jobs.
```bash
condor_submit_dag -maxidle 10000 wod-iquod-qc.dag
```


#### Othe useful commands

Check the status of a job
```bash
condor_q -nobatch
```

Check held jobs
```bash
condor_q -hold
```

Cancel all jobs
```bash
condor_rm <username>
```

Cancel specific job
```bash
condor_rm <job id>
```

Check for failed jobs
```bash
cat wod-iquod-qc.dag.dagman.out | grep failed
```

Check for number of jobs completed
```bash
condor_history <username> | head
```

