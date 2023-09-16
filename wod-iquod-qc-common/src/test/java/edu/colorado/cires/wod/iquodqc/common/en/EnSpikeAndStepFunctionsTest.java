package edu.colorado.cires.wod.iquodqc.common.en;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepFunctions.conditionA;
import static edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepFunctions.conditionB;
import static edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepFunctions.conditionC;
import static edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepFunctions.determineDepthTolerance;
import static org.junit.jupiter.api.Assertions.*;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class EnSpikeAndStepFunctionsTest {

  @Test
  public void testEnSpikeAndStepCheckDetermineDepthToleranceTropics() {
    assertEquals(5D, determineDepthTolerance(299D, 0D), 0.0000001, "depthTol in tropics miscalculated in z<300m range");
    assertEquals(3.75, determineDepthTolerance(350D, 0D), 0.0000001, "depthTol in tropics not interpolated correctly between 300 and 400 m");
    assertEquals(2.5, determineDepthTolerance(450D, 0D), 0.0000001, "depthTol in tropics miscalculated in 400<z<500m range");
    assertEquals(2D, determineDepthTolerance(550D, 0D), 0.0000001, "depthTol in tropics miscalculated in 500<z<600m range");
    assertEquals(1.5, determineDepthTolerance(601D, 0D), 0.0000001, "depthTol in tropics miscalculated below 600m");
  }

  @Test
  public void testEnSpikeAndStepCheckDetermineDepthToleranceNonTropics() {
    assertEquals(5D, determineDepthTolerance(199D, 50D), 0.0000001, "depthTol in nontropics miscalculated in z<300m range");
    assertEquals(3.75, determineDepthTolerance(250D, 50D), 0.0000001, "depthTol in nontropics not interpolated correctly between 200 and 300 m");
    assertEquals(2.5, determineDepthTolerance(350D, 50D), 0.0000001, "depthTol in nontropics miscalculated in 300<z<400m range");
    assertEquals(2D, determineDepthTolerance(550D, 50D), 0.0000001, "depthTol in nontropics miscalculated in 500<z<600m range");
    assertEquals(1.5, determineDepthTolerance(601D, 50D), 0.0000001, "depthTol in nontropics miscalculated below 600m");
  }

  @Test
  public void testEnSpikeAndStepCheckConditionA() throws Exception {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(123)
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

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);
    List<Double> dt = initializer.getDt();
    Set<Integer> failures = new LinkedHashSet<>();
    double wt1 = 0D;
    for (int i = 1; i < 4; i++) {
      double dTTol = determineDepthTolerance(cast.getDepths().get(i - 1).getDepth(), Math.abs(cast.getLatitude()));
      conditionA(dt, dTTol, failures, wt1, i, false);
    }

    assertEquals(new LinkedHashSet<>(Arrays.asList(1)), failures);

  }

  @Test
  public void testEnSpikeAndStepCheckConditionASmallSpike() throws Exception {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(123)
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

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);
    List<Double> dt = initializer.getDt();
    Set<Integer> failures = new LinkedHashSet<>();
    double wt1 = 0D;
    for (int i = 1; i < 4; i++) {
      double dTTol = determineDepthTolerance(cast.getDepths().get(i - 1).getDepth(), Math.abs(cast.getLatitude()));
      conditionA(dt, dTTol, failures, wt1, i, false);
    }

    assertEquals(new LinkedHashSet<>(), failures);

  }

  @Test
  public void testEnSpikeAndStepCheckConditionB() throws Exception {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(123)
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

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);
    List<Double> dt = initializer.getDt();
    List<Double> gt = initializer.getGt();
    Set<Integer> failures = new LinkedHashSet<>();
    double gTTol = 0.05;
    for (int i = 1; i < 4; i++) {
      double dTTol = determineDepthTolerance(cast.getDepths().get(i - 1).getDepth(), Math.abs(cast.getLatitude()));
      conditionB(dt, dTTol, gTTol, failures, i, false, gt);
    }

    assertEquals(new LinkedHashSet<>(Arrays.asList(1)), failures);

  }

  @Test
  public void testEnSpikeAndStepCheckConditionC() throws Exception {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(123)
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

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);
    List<Double> dt = initializer.getDt();
    double[] z = initializer.getZ();
    List<Double> t = initializer.getT();
    Set<Integer> failures = new LinkedHashSet<>();
    for (int i = 1; i < 4; i++) {
      double dTTol = determineDepthTolerance(cast.getDepths().get(i - 1).getDepth(), Math.abs(cast.getLatitude()));
      conditionC(dt, dTTol, z, failures, t, i, true);
    }

    assertEquals(new LinkedHashSet<>(Arrays.asList(1, 2)), failures);

  }

  @Test
  public void testEnSpikeAndStepCheckConditionCExceptionI() {
    Cast cast = Cast.builder()
        .withLatitude(20D)
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
                    .withVariable(TEMPERATURE).withValue(2D)
                    .build()))
                .build(),
            Depth.builder().withDepth(340D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(-9D)
                    .build()))
                .build()
        )).build();

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);
    List<Double> dt = initializer.getDt();
    double[] z = initializer.getZ();
    List<Double> t = initializer.getT();
    Set<Integer> failures = new LinkedHashSet<>();
    for (int i = 1; i < 4; i++) {
      double dTTol = determineDepthTolerance(cast.getDepths().get(i - 1).getDepth(), Math.abs(cast.getLatitude()));
      conditionC(dt, dTTol, z, failures, t, i, true);
    }

    assertEquals(new LinkedHashSet<>(), failures);
  }

  @Test
  public void testEnSpikeAndStepCheckConditionCExceptionIi() {
    Cast cast = Cast.builder()
        .withLatitude(20D)
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(13D)
                    .build()))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(13D)
                    .build()))
                .build(),
            Depth.builder().withDepth(30D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(2D)
                    .build()))
                .build(),
            Depth.builder().withDepth(40D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(2D)
                    .build()))
                .build()
        )).build();

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);
    List<Double> dt = initializer.getDt();
    double[] z = initializer.getZ();
    List<Double> t = initializer.getT();
    Set<Integer> failures = new LinkedHashSet<>();
    for (int i = 1; i < 4; i++) {
      double dTTol = determineDepthTolerance(cast.getDepths().get(i - 1).getDepth(), Math.abs(cast.getLatitude()));
      conditionC(dt, dTTol, z, failures, t, i, true);
    }

    assertEquals(new LinkedHashSet<>(), failures);
  }


}