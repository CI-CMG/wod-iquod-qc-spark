package edu.colorado.cires.wod.postprocess.addresulttocast;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import edu.colorado.cires.wod.postprocess.PostProcessor;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class AddResultToCastPostProcessor extends PostProcessor<Cast> {

  private static final long serialVersionUID = 0L;

  @Override
  protected Dataset<Cast> processDatasets(Dataset<Cast> castDataset, Dataset<CastCheckResult> castCheckResultDataset) {
    return castDataset.join(castCheckResultDataset, castDataset.col("castNumber").equalTo(castCheckResultDataset.col("castNumber")), "left_outer")
        .map((MapFunction<Row, Cast>) this::addResultInfoToCast, Encoders.bean(Cast.class));
  }
  
  private Cast addResultInfoToCast(Row row) {
    Cast cast = Cast.builder(row).build();
    CastCheckResult castCheckResult = CastCheckResult.builder(row).build();
    
    int castNumber = cast.getCastNumber();

    List<Depth> depths = cast.getDepths();
    int depthsSize = depths.size();
    List<Integer> iquodFlags = castCheckResult.getIquodFlags();

    if (iquodFlags.size() != depthsSize) {
      String message = String.format("Number of depths does not match number of QC flags for cast: %s", castNumber);
      throw new IllegalStateException(message);
    }

    return Cast.builder(cast)
        .withDepths(IntStream.range(0, depthsSize).boxed()
            .map(i -> {
                  Depth depth = depths.get(i);
                  List<ProfileData> data = depth.getData();
                  return Depth.builder(depth)
                      .withData(data.stream()
                          .map(profileData -> ProfileData.builder(profileData)
                              .withQcFlag(iquodFlags.get(i))
                              .build()
                          ).collect(Collectors.toList())
                      ).build();
                }
            ).collect(Collectors.toList())
        ).build();
  }
}
