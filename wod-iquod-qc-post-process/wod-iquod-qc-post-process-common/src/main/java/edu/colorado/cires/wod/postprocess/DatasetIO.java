package edu.colorado.cires.wod.postprocess;

import java.util.Arrays;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DatasetIO {
  
  private static final String JSON_EXT = ".json";
  private static final String PARQUET_EXT = ".parquet";

  public static <T> Dataset<T> readDataset(String[] uris, SparkSession sparkSession, Class<T> tClass) {
    if (Arrays.stream(uris).anyMatch(u -> !u.endsWith(PARQUET_EXT))) {
      throw new IllegalArgumentException("Reading is only supported for parquet format");
    }
    return DatasetConverter.convert(sparkSession.read().parquet(uris), tClass);
  }
  public static boolean writeDataset(String uri, Dataset<?> dataset, SaveMode saveMode, long maxRecordsPerFile) {
    return write(
        getWriter(saveMode, maxRecordsPerFile, dataset),
        uri
    );
  }

  public static boolean writeDatasetInPartitions(String uri, Dataset<?> dataset, SaveMode saveMode, long maxRecordsPerFile, String[] partitionFields) {
    return write(
        getWriter(saveMode, maxRecordsPerFile, dataset)
            .partitionBy(partitionFields),
        uri
    );
  }
  
  private static DataFrameWriter<?> getWriter(SaveMode saveMode, long maxRecordsPerFile, Dataset<?> dataset) {
    return dataset.write()
        .mode(saveMode)
        .option("maxRecordsPerFile", maxRecordsPerFile);
  }
  
  private static boolean write(DataFrameWriter<?> writer, String uri) {
    if (uri.endsWith(JSON_EXT)) {
      writer.json(uri);
    } else if (uri.endsWith(PARQUET_EXT)) {
      writer.parquet(uri);
    } else {
      throw new IllegalArgumentException("Writing is only supported for json or parquet format");
    }
    
    return true;
  }

}
