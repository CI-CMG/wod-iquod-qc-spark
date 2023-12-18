package edu.colorado.cires.wod.iquodqc.check.profileenvelop;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class GTSPPProfileEnvelopCheckTest {
  
  @Test void testPass() {
    Cast cast = Cast.builder()
        .withDepths(
            List.of(
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(35)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(10)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(250)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(28)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(5600)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(2)
                                .build()
                        )
                    )
                    .build()
            )
        )
        .build();

    assertEquals(Collections.emptyList(), new GTSPPProfileEnvelopCheck().getFailedDepths(cast));
  }
  
  @Test void testFail() {
    Cast cast = Cast.builder()
        .withDepths(
            List.of(
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(35)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(38)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(250)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(28)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(5600)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(10)
                                .build()
                        )
                    )
                    .build()
            )
        )
        .build();

    assertEquals(List.of(0, 2), new GTSPPProfileEnvelopCheck().getFailedDepths(cast));
  }

}
