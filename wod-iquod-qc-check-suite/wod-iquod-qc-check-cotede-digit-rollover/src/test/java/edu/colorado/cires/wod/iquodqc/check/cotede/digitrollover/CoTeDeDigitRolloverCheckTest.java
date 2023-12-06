package edu.colorado.cires.wod.iquodqc.check.cotede.digitrollover;

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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class CoTeDeDigitRolloverCheckTest {
  
  private static final double[] FAILING_VALUES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};


  @Test void testDigitRolloverFromCastTemperatureFailure() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(t -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                          .withVariableCode(TEMPERATURE).withValue(t)
                          .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(1D)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(1D)
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeDigitRolloverCheck().getFailedDepths(cast);
    assertEquals(5, results.size());
    assertEquals(List.of(7, 8, 9, 10, 14), results);
  }

  @Test void testDigitRolloverFromCastPressureFailure() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(p -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(1D)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(p)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(1D)
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeDigitRolloverCheck().getFailedDepths(cast);
    assertEquals(5, results.size());
    assertEquals(List.of(7, 8, 9, 10, 14), results);
  }

  @Test void testDigitRolloverFromCastSalinityFailure() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(s -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(1D)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(1D)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(s)
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeDigitRolloverCheck().getFailedDepths(cast);
    assertEquals(5, results.size());
    assertEquals(List.of(7, 8, 9, 10, 14), results);
  }

  @Test void testDigitRolloverFromCastMixedFailure() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(v -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(v)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(1D)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(v)
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeDigitRolloverCheck().getFailedDepths(cast);
    assertEquals(5, results.size());
    assertEquals(List.of(7, 8, 9, 10, 14), results);
  }

  @Test void testDigitRolloverFromCastPass() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(v -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(ThreadLocalRandom.current().nextDouble(0, 2.5))
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(ThreadLocalRandom.current().nextDouble(0, 2.5))
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(ThreadLocalRandom.current().nextDouble(0, 2.5))
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeDigitRolloverCheck().getFailedDepths(cast);
    assertEquals(0, results.size());
  }

}
