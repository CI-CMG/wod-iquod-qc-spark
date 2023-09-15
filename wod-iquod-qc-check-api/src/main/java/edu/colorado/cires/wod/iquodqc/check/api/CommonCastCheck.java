package edu.colorado.cires.wod.iquodqc.check.api;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udf;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

public abstract class CommonCastCheck implements CastCheck, Serializable {

  @Override
  public Dataset<CastCheckResult> joinResultDataset(CastCheckContext context) {
    registerUdf(context);
    return convertResultToDataset(executeQuery(context));
  }

  protected boolean doGetDependsOnResults() {
    return true;
  }

  protected void registerUdf(CastCheckContext context) {
    SparkSession spark = context.getSparkSession();
    spark.udf().register(getName(), udf((UDF1<Row, Row>) this::checkUdf, CastCheckResult.structType()));
  }

  private static Column[] resolveColumns(Dataset<Cast> castDataset, Map<String, Dataset<CastCheckResult>> otherResultDatasets) {
    Column[] columns = new Column[otherResultDatasets.size() + 1];
    columns[0] = struct(castDataset.col("*")).as("cast");
    int i = 1;
    for (Entry<String, Dataset<CastCheckResult>> entry : otherResultDatasets.entrySet()) {
      columns[i++] = struct(entry.getValue().col("*")).as(entry.getKey());
    }
    return columns;
  }

  protected Dataset<Row> executeQuery(CastCheckContext context) {
    Dataset<Cast> castDataset = context.readCastDataset();
    Map<String, Dataset<CastCheckResult>> otherResultDatasets = new HashMap<>();
    Dataset<Row> joined = null;
    if (doGetDependsOnResults()) {
      for (String otherTestName : dependsOn()) {
        Dataset<CastCheckResult> otherResultDataset = context.readCastCheckResultDataset(otherTestName);
        joined = (joined == null ? castDataset : joined).join(otherResultDataset,
            castDataset.col("castNumber").equalTo(otherResultDataset.col("castNumber")), "left_outer");
        otherResultDatasets.put(otherTestName, otherResultDataset);
      }
    }
    joined = (joined == null ? castDataset : joined).select(resolveColumns(castDataset, otherResultDatasets));
    return joined.select(callUDF(getName(), struct(col("*"))).as("result"));
  }

  protected Dataset<CastCheckResult> convertResultToDataset(Dataset<Row> rows) {
    return rows.select("result.*").as(Encoders.bean(CastCheckResult.class));
  }

  protected Row checkUdf(Row row) {
    Row castRow = row.getStruct(row.fieldIndex("cast"));
    Map<String, CastCheckResult> otherTestResults = new HashMap<>();
    if (doGetDependsOnResults()) {
      for (String otherTestName : dependsOn()) {
        CastCheckResult otherTestResult = CastCheckResult.builder(row.getStruct(row.fieldIndex(otherTestName))).build();
        otherTestResults.put(otherTestName, otherTestResult);
      }
    }
    return checkCast(Cast.builder(castRow).build(), otherTestResults).asRow();
  }

  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    return getFailedDepths(cast);
  }

  protected abstract Collection<Integer> getFailedDepths(Cast cast);

  protected CastCheckResult checkCast(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    Collection<Integer> failed = getFailedDepths(cast, otherTestResults);
    return CastCheckResult.builder()
        .withCastNumber(cast.getCastNumber())
        .withPassed(failed.isEmpty())
        .withFailedDepths(new ArrayList<>(failed))
        .build();
  }

}
