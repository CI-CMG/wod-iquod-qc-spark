package edu.colorado.cires.wod.postprocess;

import java.util.Arrays;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DatasetIO {
  
  private static final String JSON_EXT = ".json";
  private static final String PARQUET_EXT = ".parquet";

  public static <T> Dataset<T> readDataset(String[] uris, SparkSession sparkSession, Class<T> tClass) {
    if (Arrays.stream(uris).anyMatch(u -> !u.endsWith(PARQUET_EXT))) {
      throw new IllegalArgumentException("Reading is only supported for parquet format");
    }
    System.out.println("Reading dataset from: " + Arrays.toString(uris));
    return DatasetConverter.convert(sparkSession.read().parquet(uris), tClass);
  }
  public static void writeDataset(String uri, Dataset<?> dataset, SaveMode saveMode, long maxRecordsPerFile) {
    System.out.println("Writing dataset to: " + uri);
    
    DataFrameWriter<?> dataFrameWriter = dataset.write()
        .mode(saveMode)
        .option("maxRecordsPerFile", maxRecordsPerFile);
    
    if (uri.endsWith(JSON_EXT)) {
      dataFrameWriter.json(uri);
    } else if (uri.endsWith(PARQUET_EXT)) {
      dataFrameWriter.parquet(PARQUET_EXT);
    } else {
      throw new IllegalArgumentException("Writing is only supported for json or parquet format");
    }
  }

}
