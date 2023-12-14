package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.SALINITY;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class CoTeDeSpikeGTSPPCheckTest {

  private static final double[] VALUES = {25.32, 25.34, Double.NaN, 25.31, 24.99, 23.46, 21.85, 20.95,
      19.35, 20.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};

  @Test
  void testSpikeFromCastTemperatureFailure() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(VALUES)
                .mapToObj(t -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(t)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(1)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(1)
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeSpikeGTSPPCheck().getFailedDepths(cast);
    assertEquals(3, results.size());
    assertEquals(List.of(2, 12, 14), results);
  }

  @Test void testSpikeFromCastPass() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(VALUES)
                .mapToObj(v -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(1)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(1)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(1)
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeSpikeGTSPPCheck().getFailedDepths(cast);
    assertEquals(0, results.size());
  }

}
