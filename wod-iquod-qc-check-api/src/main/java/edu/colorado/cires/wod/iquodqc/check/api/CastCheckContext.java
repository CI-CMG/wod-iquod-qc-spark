package edu.colorado.cires.wod.iquodqc.check.api;

import edu.colorado.cires.wod.parquet.model.Cast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public interface CastCheckContext {

  SparkSession getSparkSession();
//  String resolveCheckDatasetUri(String checkName);
//  String getCheckResultDatasetUri();
//  String getCastDatasetUri();
  Dataset<Cast> readCastDataset();

}
