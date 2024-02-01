package edu.colorado.cires.wod.postprocess;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class DatasetConverter {

  public static <T> Dataset<T> convert(Dataset<Row> rowDataset, Class<T> tClass) {
    return rowDataset.as(Encoders.bean(tClass));
  }

}
