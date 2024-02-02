package edu.colorado.cires.wod.postprocess.summaryreport;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
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
    Dataset<Row> castRowDataset = castDataset.select(
        col("castNumber").as("castCastNumber"),
        col("*")
    ).drop("castNumber");

    resultRowDataset = resultRowDataset.join(
        castRowDataset,
        resultRowDataset.col("castNumber").equalTo(castRowDataset.col("castCastNumber"))
    );

    Dataset<Row> failedDataset = resultRowDataset.filter(col("passed").equalTo(false));
    if (failedDataset.count() == 0) {
      return resultRowDataset.groupBy(lit(1)).agg(
              expr("any_value(dataset)").as("dataset"),
              expr("any_value(year)").as("year"),
              lit(totalExceptions).as("exceptionCount"),
              lit(totalCasts).as("totalProfiles")
          ).withColumn(
              "failureCounts",
              map_from_arrays(array(), array())
          ).drop("1")
          .as(Encoders.bean(Summary.class));
    }

    return failedDataset
        .groupBy("checkName").agg(
            count(col("*")).as("failureCount"),
            expr("any_value(dataset)").as("dataset"),
            expr("any_value(year)").as("year")
        ).groupBy(lit(1)).agg(
            expr("any_value(dataset)").as("dataset"),
            expr("any_value(year)").as("year"),
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
