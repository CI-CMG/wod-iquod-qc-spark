package edu.colorado.cires.wod.iquodqc.common.en;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.*;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class EnSpikeAndStepCheckerTest {

  @Test
  public void testEnSpikeAndStepCheckTropicsPrelim() {
    Cast cast = Cast.builder()
        .withLatitude(0D)
        .withCastNumber(2222)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build(),
            Depth.builder().withDepth(30D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build()
        )).build();

    assertEquals(Arrays.asList(0,1,2,3), new ArrayList<>(EnSpikeAndStepChecker.getFailedDepths(cast, true)));
  }

  @Test
  public void testEnSpikeAndStepCheckANominal() {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(20D)
                    .build()))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(24D)
                    .build()))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(18D)
                    .build()))
                .build(),
            Depth.builder().withDepth(30D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(17D)
                    .build()))
                .build()
        )).build();

    assertEquals(Arrays.asList(1), new ArrayList<>(EnSpikeAndStepChecker.getFailedDepths(cast, false)));
  }

  @Test
  public void testEnSpikeAndStepCheckSpikeADepthConstraintShallow() {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(21D)
                    .build()))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(24D)
                    .build()))
                .build(),
            Depth.builder().withDepth(70D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(18D)
                    .build()))
                .build(),
            Depth.builder().withDepth(80D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(17D)
                    .build()))
                .build()
        )).build();

    assertEquals(Arrays.asList(), new ArrayList<>(EnSpikeAndStepChecker.getFailedDepths(cast, false)));
  }

  @Test
  public void testEnSpikeAndStepCheckSpikeADepthConstraintDeep() {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(500D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(20D)
                    .build()))
                .build(),
            Depth.builder().withDepth(510D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(21D)
                    .build()))
                .build(),
            Depth.builder().withDepth(670D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(18D)
                    .build()))
                .build(),
            Depth.builder().withDepth(680D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(17D)
                    .build()))
                .build()
        )).build();

    assertEquals(Arrays.asList(), new ArrayList<>(EnSpikeAndStepChecker.getFailedDepths(cast, false)));
  }

  @Test
  public void testEnSpikeAndStepCheckBNominal() {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(500D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(22.5)
                    .build()))
                .build(),
            Depth.builder().withDepth(510D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(24D)
                    .build()))
                .build(),
            Depth.builder().withDepth(520D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(22.5)
                    .build()))
                .build(),
            Depth.builder().withDepth(530D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(22D)
                    .build()))
                .build()
        )).build();

    assertEquals(Arrays.asList(1), new ArrayList<>(EnSpikeAndStepChecker.getFailedDepths(cast, false)));
  }

  @Test
  public void testEnSpikeAndStepCheckCNominal() {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(24D)
                    .build()))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(24D)
                    .build()))
                .build(),
            Depth.builder().withDepth(30D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(2D)
                    .build()))
                .build(),
            Depth.builder().withDepth(40D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(1D)
                    .build()))
                .build()
        )).build();

    assertEquals(Arrays.asList(1,2), new ArrayList<>(EnSpikeAndStepChecker.getFailedDepths(cast, true)));
  }

  @Test
  public void testEnSpikeAndStepCheckExcpetionCIii() {
    Cast cast = Cast.builder()
        .withLatitude(50D)
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(310D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(13D)
                    .build()))
                .build(),
            Depth.builder().withDepth(320D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(13D)
                    .build()))
                .build(),
            Depth.builder().withDepth(330D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(13D)
                    .build()))
                .build(),
            Depth.builder().withDepth(340D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(1D)
                    .build()))
                .build()
        )).build();

    assertEquals(Arrays.asList(3), new ArrayList<>(EnSpikeAndStepChecker.getFailedDepths(cast, true)));
  }

  @Test
  public void testEnSpikeAndStepCheckTrailingZero() {
    Cast cast = Cast.builder()
        .withLatitude(50D)
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build(),
            Depth.builder().withDepth(30D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build(),
            Depth.builder().withDepth(40D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build()
        )).build();

    assertEquals(Arrays.asList(3), new ArrayList<>(EnSpikeAndStepChecker.getFailedDepths(cast, true)));
  }

}