package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.SALINITY;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getTemperatures;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class CoTeDeGradientCheckTest {

  private static final double[] VALUES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};

  @Test
  void testGradientFromCastTemperatureFailure() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(VALUES)
                .mapToObj(t -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(t)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(ThreadLocalRandom.current().nextDouble(0, 1.5))
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(ThreadLocalRandom.current().nextDouble(0, 1.5))
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeGradientCheck().getFailedDepths(
        cast,
        new HashMap<>(0)
    );
    assertEquals(3, results.size());
    assertEquals(List.of(10, 11, 14), results);
  }

  @Test void testGradientFromCastPass() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(VALUES)
                .mapToObj(ignored -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(ThreadLocalRandom.current().nextDouble(0, 1.5))
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(ThreadLocalRandom.current().nextDouble(0, 1.5))
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(ThreadLocalRandom.current().nextDouble(0, 1.5))
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeGradientCheck().getFailedDepths(
        cast,
        new HashMap<>(0)
    );
    assertEquals(0, results.size());
  }

}
