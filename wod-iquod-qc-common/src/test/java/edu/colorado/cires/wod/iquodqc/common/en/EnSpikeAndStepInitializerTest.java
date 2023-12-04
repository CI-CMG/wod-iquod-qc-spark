package edu.colorado.cires.wod.iquodqc.common.en;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class EnSpikeAndStepInitializerTest {

  @Test
  public void testEnSpikeAndStepCheckComposeDtNominal() throws Exception {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariableCode(TEMPERATURE).withValue(20D)
                    .build()))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariableCode(TEMPERATURE).withValue(24D)
                    .build()))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariableCode(TEMPERATURE).withValue(18D)
                    .build()))
                .build(),
            Depth.builder().withDepth(30D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariableCode(TEMPERATURE).withValue(17D)
                    .build()))
                .build()
        )).build();

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);

    assertEquals(Arrays.asList(null, 4D, -6D, -1D), initializer.getDt());

  }

  @Test
  public void testEnSpikeAndStepCheckComposeDtGap() throws Exception {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariableCode(TEMPERATURE).withValue(20D)
                    .build()))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariableCode(TEMPERATURE).withValue(24D)
                    .build()))
                .build(),
            Depth.builder().withDepth(70D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariableCode(TEMPERATURE).withValue(18D)
                    .build()))
                .build(),
            Depth.builder().withDepth(80D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariableCode(TEMPERATURE).withValue(17D)
                    .build()))
                .build()
        )).build();

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);

    assertEquals(Arrays.asList(null, 4D, null, -1D), initializer.getDt());

  }

}