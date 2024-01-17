package edu.colorado.cires.wod.iquodqc.check.api;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udf;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

public abstract class CommonCastCheck implements CastCheck, Serializable {

  private static final int TEMPERATURE = 1;

  @Override
  public Dataset<CastCheckResult> joinResultDataset(CastCheckContext context) {
    return convertResultToDataset(executeQuery(context));
  }

  protected boolean doGetDependsOnResults() {
    return true;
  }

  protected void registerUdf(CastCheckContext context) {
    SparkSession spark = context.getSparkSession();
    spark.udf().register(getName(), udf((UDF1<Row, Row>) this::checkUdf, CastCheckResult.structType()));
  }

  protected static Column[] resolveColumns(Dataset<Cast> castDataset, Map<String, Dataset<CastCheckResult>> otherResultDatasets) {
    Column[] columns = new Column[otherResultDatasets.size() + 1];
    columns[0] = struct(castDataset.col("*")).as("cast");
    int i = 1;
    for (Map.Entry<String, Dataset<CastCheckResult>> entry : otherResultDatasets.entrySet()) {
      columns[i++] = struct(entry.getValue().col("*")).as(entry.getKey());
    }
    return columns;
  }

  protected Dataset<Row> createQuery(CastCheckContext context) {
    registerUdf(context);
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
    return (joined == null ? castDataset : joined).select(resolveColumns(castDataset, otherResultDatasets));
  }

  protected Dataset<Row> selectCallUdf(Dataset<Row> queryDataset) {
    return queryDataset.select(callUDF(getName(), struct(col("*"))).as("result"));
  }

  protected Dataset<Row> executeQuery(CastCheckContext context) {
    return selectCallUdf(createQuery(context));
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
    Cast cast = filterFlags(Cast.builder(castRow).build());
    CastCheckResult filter = filterCast(cast);
    if (filter != null) {
      return filter.asRow();
    }
    return checkCast(cast, otherTestResults).asRow();
  }


  protected Cast filterFlags(Cast cast) {
    /*
      In some IQuOD datasets temperature values of 99.9 or 99.99 are special values to
      signify not to use the data value. These are flagged here so they are not
      sent to the quality control programs for testing.
     */
    return Cast.builder(cast)
        .withDepths(cast.getDepths().stream()
            .map(depth -> Depth.builder(depth).withData(filterProfileData(depth)).build())
            .collect(Collectors.toList())).build();
  }

  protected CastCheckResult filterCast(Cast cast) {
    String filterReason = ProfileFilter.getFilterReason(cast);
    if (filterReason != null) {
      return CastCheckResult.builder()
          .withCastNumber(cast.getCastNumber())
          .withFiltered(true)
          .withFilterReason(filterReason)
          .withPassed(true)
          .build();
    }
    return null;
  }

  private static List<ProfileData> filterProfileData(Depth depth) {
    return depth.getData().stream().filter(CommonCastCheck::isValidProfileData).collect(Collectors.toList());
  }

  private static boolean isValidProfileData(ProfileData pd) {
    return pd.getVariableCode() != TEMPERATURE || (pd.getVariableCode() == TEMPERATURE && isValidTemperature(pd.getValue()));
  }

  private static boolean isValidTemperature(double temp) {
    return temp < 99D || temp >= 100D;
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
        .withDependsOnFailedDepths(
            getDependsOnFailedDepths(otherTestResults)
        ).withDependsOnFailedChecks(
            getDependsOnFailedChecks(otherTestResults)
        )
        .build();
  }

  private static Map<String, List<Integer>> getDependsOnFailedDepths(Map<String, CastCheckResult> otherTestResults) {
    Map<String, List<Integer>> doFailedDepths = new HashMap<>(0);

    for (Map.Entry<String, CastCheckResult> entry : otherTestResults.entrySet()) {
      CastCheckResult castCheckResult = entry.getValue();

      List<Integer> failedDepths = castCheckResult.getFailedDepths();
      if (!failedDepths.isEmpty()) {
        doFailedDepths.put(entry.getKey(), failedDepths);
      }

      doFailedDepths.putAll(castCheckResult.getDependsOnFailedDepths());
    }

    return doFailedDepths;
  }

  private static Map<String, List<String>> getDependsOnFailedChecks(Map<String, CastCheckResult> otherTestResults) {
    Map<String, List<String>> doFailedChecks = new HashMap<>(0);

    for (Map.Entry<String, CastCheckResult> entry : otherTestResults.entrySet()) {
      CastCheckResult castCheckResult = entry.getValue();
      Map<String, List<Integer>> failedDepths = castCheckResult.getDependsOnFailedDepths();

      if (!failedDepths.isEmpty()) {
        doFailedChecks.put(entry.getKey(), new ArrayList<>(failedDepths.keySet()));
      }

      doFailedChecks.putAll(castCheckResult.getDependsOnFailedChecks());
    }

    return doFailedChecks;
  }

}
