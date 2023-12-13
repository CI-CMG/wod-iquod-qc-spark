package edu.colorado.cires.wod.iquodqc.check.cotede.globalrange;

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

public class CoTeDeGlobalRangeGTSPPCheckTest {

  private static final double[] FAILING_VALUES = {-3,-20000,3,4,8,41,50000000,5,Double.NaN,7};


  @Test void testGlobalRangeGTSPPFromCastTemperatureFailure() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(t -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                          .withVariableCode(TEMPERATURE).withValue(t)
                          .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeGlobalRangeGTSPPCheck().getFailedDepths(cast);
    assertEquals(5, results.size());
    assertEquals(List.of(0,1,5,6,8), results);
  }

  @Test void testGlobalRangeGTSPPFromCastPass() {
    Cast cast = Cast.builder()
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(v -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(ThreadLocalRandom.current().nextDouble(0, 2.5))
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Collection<Integer> results = new CoTeDeGlobalRangeGTSPPCheck().getFailedDepths(cast);
    assertEquals(0, results.size());
  }

}
