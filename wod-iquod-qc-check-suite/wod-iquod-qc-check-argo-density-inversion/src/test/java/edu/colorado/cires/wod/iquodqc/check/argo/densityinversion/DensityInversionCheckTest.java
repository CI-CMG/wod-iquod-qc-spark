package edu.colorado.cires.wod.iquodqc.check.argo.densityinversion;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.SALINITY;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class DensityInversionCheckTest {

  private static final double[] P = {2, 6, 10, 21, 44, 79, 100, 150,
      200, 400, 410, 650, 1000, 2000, 5000};
  private static final double[] T = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final double[] S = {36.49, 36.51, 36.52, 36.53, 36.59, 36.76, 36.81, 36.39,
      35.98, 35.30, 35.28, 34.93, 34.86, Double.NaN, Double.NaN};
  
  @Test void testFailureFlags() {
    Cast cast = Cast.builder()
        .withDepths(
            IntStream.range(0, P.length).boxed()
                .map(i ->
                    Depth.builder()
                        .withData(
                            List.of(
                                ProfileData.builder()
                                    .withVariableCode(PRESSURE)
                                    .withValue(P[i])
                                    .build(),
                                ProfileData.builder()
                                    .withVariableCode(TEMPERATURE)
                                    .withValue(T[i])
                                    .build(),
                                ProfileData.builder()
                                    .withVariableCode(SALINITY)
                                    .withValue(S[i])
                                    .build()
                            )
                        )
                        .build()
                ).collect(Collectors.toList())
        )
        .build();
    
    Collection<Integer> failures = new DensityInversionCheck().getFailedDepths(cast);
    assertEquals(List.of(11), failures);
  }

}
