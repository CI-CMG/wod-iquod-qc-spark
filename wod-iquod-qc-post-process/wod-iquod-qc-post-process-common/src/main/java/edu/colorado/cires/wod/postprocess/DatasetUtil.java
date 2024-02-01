package edu.colorado.cires.wod.postprocess;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.reverse;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.substring_index;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class DatasetUtil {
  
  public static Dataset<Row> addCheckNameToCastCheckResultDataset(Dataset<CastCheckResult> castCheckResultDataset) {
    Dataset<Row> dataset = castCheckResultDataset.select(
        input_file_name().as("parquetSource"),
        col("*"));

    dataset = dataset
        .select(
            reverse(split(col("parquetSource"), "/")).as("splitParquetSource"),
            col("*")
        )
        .drop("parquetSource");

    dataset = dataset
        .select((
                element_at(col("splitParquetSource"), 2)).as("parquetFileName"),
            col("*")
        ).drop("splitParquetSource");

    dataset = dataset
        .select(
            substring_index(col("parquetFileName"), ".", 1).as("checkName"),
            col("*")
        ).drop("parquetFileName");
    
    return dataset;
  }

}
