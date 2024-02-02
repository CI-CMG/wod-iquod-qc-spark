package edu.colorado.cires.wod.postprocess.failurereport;

import static org.apache.spark.sql.functions.array_join;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.map_from_arrays;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.Failures;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.postprocess.DatasetUtil;
import edu.colorado.cires.wod.postprocess.PostProcessor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.collection.Seq;

public class FailureReportPostProcessor extends PostProcessor<Failures> {
  
  private static final long serialVersionUID = 0L;
  
  @Override
  protected Dataset<Failures> processDatasets(Dataset<Cast> castDataset, Dataset<CastCheckResult> castCheckResultDataset) {
    Dataset<Row> resultDataset = DatasetUtil.addCheckNameToCastCheckResultDataset(castCheckResultDataset);

    Dataset<Row> castRowDataset = castDataset.select(
        col("castNumber").as("castCastNumber"),
        col("*")
    ).drop("castNumber");

    castRowDataset = castRowDataset.join(
        resultDataset,
        castRowDataset.col("castCastNumber").equalTo(resultDataset.col("castNumber"))
    );

    Dataset<Row> grouped = castRowDataset.groupBy("castNumber").agg(
        expr("any_value(size(depths))").as("numberOfDepths"),
        expr("any_value(dataset)").as("dataset"),
        expr("any_value(year)").as("year"),
        array_join(
            collect_list("errorMessage"),
            ", "
        ).as("exception"),
        map_from_arrays(
            collect_list("checkName"),
            collect_list("failedDepths")
        ).as("failuresAtDepth"),
        flatten(collect_list("iquodFlags")).as("iquodFlags")
    );

    return grouped.map((MapFunction<Row, Failures>) this::createFailuresFromRow, Encoders.bean(Failures.class));
  }
  
  private Failures createFailuresFromRow(Row row) {
    Map<String, Seq<Integer>> failuresAtDepthMap = row.getJavaMap(row.fieldIndex("failuresAtDepth"));
    int numberOfDepths = row.getInt(row.fieldIndex("numberOfDepths"));

    List<List<String>> depthFailures = new ArrayList<>();
    Set<String> profileFailures = new HashSet<>(0);
    for (int i = 0; i < numberOfDepths; i++) {
      depthFailures.add(new ArrayList<>(0));
    }

    for (Entry<String, Seq<Integer>> entry : failuresAtDepthMap.entrySet()) {
      Seq<Integer> indices = entry.getValue();
      String checkName = entry.getKey();
      if (!indices.isEmpty()) {
        profileFailures.add(checkName);
        for (int i = 0; i < indices.size(); i++) {
          int index = indices.apply(i);
          depthFailures.get(index).add(checkName);
        }
      }
    }

    String exception = row.getString(row.fieldIndex("exception"));

    return Failures.builder()
        .withCastNumber(row.getInt(row.fieldIndex("castNumber")))
        .withDataset(row.getString(row.fieldIndex("dataset")))
        .withYear(row.getInt(row.fieldIndex("year")))
        .withException(exception != null && exception.isBlank() ? null : exception)
        .withProfileFailures(new ArrayList<>(profileFailures))
        .withIquodFlags(row.getList(row.fieldIndex("iquodFlags")))
        .withDepthFailures(depthFailures)
        .build();
  }
}
