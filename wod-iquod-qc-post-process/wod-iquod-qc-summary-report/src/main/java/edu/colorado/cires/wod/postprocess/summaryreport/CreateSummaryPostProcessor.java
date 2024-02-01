package edu.colorado.cires.wod.postprocess.summaryreport;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map_from_arrays;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.Summary;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.postprocess.DatasetUtil;
import edu.colorado.cires.wod.postprocess.PostProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class CreateSummaryPostProcessor extends PostProcessor<Summary> {

  private static final long serialVersionUID = 0L;
  
  @Override
  protected Dataset<Summary> processDatasets(Dataset<Cast> castDataset, Dataset<CastCheckResult> castCheckResultDataset) {
    long totalCasts = castDataset.count();
    long totalExceptions = castCheckResultDataset.filter(col("error").equalTo(true)).count();
    
    Dataset<Row> resultRowDataset = DatasetUtil.addCheckNameToCastCheckResultDataset(castCheckResultDataset);

    return resultRowDataset.filter(col("passed").equalTo(false))
        .groupBy("checkName").agg(
            count(col("*")).as("failureCount")
        ).groupBy(lit(1)).agg(
            collect_list("checkName").as("checks"),
            collect_list("failureCount").as("failures"),
            lit(totalExceptions).as("exceptionCount"),
            lit(totalCasts).as("totalProfiles")
        ).withColumn(
            "failureCounts",
            map_from_arrays(col("checks"), col("failures"))
        ).drop("1", "checks", "failures")
        .as(Encoders.bean(Summary.class));
  }
}
