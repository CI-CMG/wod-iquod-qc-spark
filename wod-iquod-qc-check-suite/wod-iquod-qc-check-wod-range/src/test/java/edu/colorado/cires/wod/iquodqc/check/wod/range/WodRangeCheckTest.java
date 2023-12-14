package edu.colorado.cires.wod.iquodqc.check.wod.range;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;

public class WodRangeCheckTest {
  
  @Test void testTemperaturePass() {
    Cast cast = Cast.builder()
        .withLatitude(-89.5)
        .withLongitude(0.5)
        .withDepths(
            List.of(
                Depth.builder().withDepth(0.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(15.)
                                .build()
                        )
                    ).build(),
                Depth.builder().withDepth(50.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(-2.4)
                                .build()
                        )
                    ).build()
            )
        ).build();
    
    assertEquals(0, new WodRangeCheck().getFailedDepths(cast).size());
  }
  
  @Test void testTemperatureFailure() {
    Cast cast = Cast.builder()
        .withLatitude(-89.5)
        .withLongitude(0.5)
        .withDepths(
            List.of(
                Depth.builder().withDepth(0.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(0.)
                                .build()
                        )
                    ).build(),
                Depth.builder().withDepth(2400)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(10.)
                                .build()
                        )
                    ).build()
            )
        ).build();

    Collection<Integer> failedCasts = new WodRangeCheck().getFailedDepths(cast);
    assertEquals(1, failedCasts.size());
    assertEquals(List.of(1), failedCasts);
  }

}
