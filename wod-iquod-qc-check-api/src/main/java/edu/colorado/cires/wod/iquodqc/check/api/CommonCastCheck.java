package edu.colorado.cires.wod.iquodqc.check.api;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udf;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

public abstract class CommonCastCheck implements CastCheck, Serializable {

  @Override
  public Dataset<CastCheckResult> joinResultDataset(CastCheckContext context) {
    SparkSession spark = context.getSparkSession();
    spark.udf().register(getName(), udf((UDF1<Row, Row>) this::checkUdf, CastCheckResult.structType()));
    Dataset<Cast> castDataset = context.readCastDataset();
    return castDataset.select(callUDF(getName(), struct(col("*"))).as("result")).select("result.*").as(Encoders.bean(CastCheckResult.class));
  }

  private Row checkUdf(Row row) {
    return checkCast(Cast.builder(row).build()).asRow();
  }

  protected abstract Collection<Integer> getFailedDepths(Cast cast);

  protected CastCheckResult checkCast(Cast cast) {
    Collection<Integer> failed = getFailedDepths(cast);
    return CastCheckResult.builder()
        .withCastNumber(cast.getCastNumber())
        .withPassed(failed.isEmpty())
        .withFailedDepths(new ArrayList<>(failed))
        .build();
  }

}
