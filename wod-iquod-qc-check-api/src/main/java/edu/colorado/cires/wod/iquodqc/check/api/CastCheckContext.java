package edu.colorado.cires.wod.iquodqc.check.api;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public interface CastCheckContext {

  SparkSession getSparkSession();
  Dataset<Cast> readCastDataset();

  default Properties getProperties(){
    return new Properties();
  }

}
