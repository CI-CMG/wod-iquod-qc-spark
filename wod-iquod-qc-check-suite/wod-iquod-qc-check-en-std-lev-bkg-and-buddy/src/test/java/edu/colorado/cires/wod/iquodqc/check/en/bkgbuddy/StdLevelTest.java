package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundChecker;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundCheckerLevelResult;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundCheckerResult;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class StdLevelTest {

  private static final List<Double> slev = Arrays.asList(
      5.0215898, 15.07854, 25.16046, 35.27829, 45.44776, 55.691494, 66.041985, 76.54591, 87.27029, 98.31118, 109.806175, 121.9519, 135.02855, 149.43373, 165.72845, 184.69746, 207.42545, 235.38617, 270.53412, 315.37408, 372.96545, 446.80093, 540.5022, 657.32294, 799.5496, 967.99585, 1161.8059, 1378.661, 1615.2905, 1868.0707, 2133.517, 2408.5835, 2690.7803, 2978.166, 3269.278, 3563.0408, 3858.6763, 4155.628, 4453.502, 4752.021, 5050.9897, 5350.272
  );

  @Test
  public void testMeanDifferencesAtStandardLevels() {
    Cast cast = Cast.builder()
        .withCastNumber(1)
        .withLatitude(-39.889)
        .withLongitude(17.650000)
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(12D)
        .withAttributes(Collections.singletonList(Attribute.builder().withCode(PROBE_TYPE).withValue(2D).build()))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(5.1).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(46).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build()
            ))
        .build();


    Map<String, CastCheckResult> otherTestResults = new HashMap<>();

    EnBackgroundCheckerResult bgResult = new EnBackgroundCheckerResult();
    bgResult.setFailures(new HashSet<>(Arrays.asList(1)));
    bgResult.getLevels().add(new EnBackgroundCheckerLevelResult(0, 5, 2));
    bgResult.getLevels().add(new EnBackgroundCheckerLevelResult(2, 45, 40));
    bgResult.getLevels().add(new EnBackgroundCheckerLevelResult(3, 46, 39));

    EnBackgroundChecker enBackgroundChecker = mock(EnBackgroundChecker.class);
    when(enBackgroundChecker.getFailedDepths(cast, otherTestResults)).thenReturn(bgResult);

    StdLevelResolver stdLevelResolver = new StdLevelResolver(enBackgroundChecker, slev);
    List<StdLevel> stdLevels = stdLevelResolver.getStdLevels(cast, otherTestResults);

    assertEquals(2, stdLevels.size());
    assertEquals(0, stdLevels.get(0).getStdLevelIndex());
    assertEquals(4, stdLevels.get(1).getStdLevelIndex());

    assertEquals(3D, stdLevels.get(0).getStdLevel(), 0.001);
    assertEquals(6D, stdLevels.get(1).getStdLevel(), 0.001);

    assertEquals(0, stdLevels.get(0).getStdLevelIndex());
    assertEquals(0, stdLevels.get(0).getLevelWrappers().get(0).getLevel().getOrigLevel());

    assertEquals(4, stdLevels.get(1).getStdLevelIndex());
    assertEquals(2, stdLevels.get(1).getLevelWrappers().get(0).getLevel().getOrigLevel());
    assertEquals(3, stdLevels.get(1).getLevelWrappers().get(1).getLevel().getOrigLevel());

  }

  @Test
  public void testFilterLevels() {
    Cast cast = Cast.builder()
        .withCastNumber(1)
        .withLatitude(-39.889)
        .withLongitude(17.650000)
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(12D)
        .withAttributes(Collections.singletonList(Attribute.builder().withCode(PROBE_TYPE).withValue(2D).build()))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(20).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(20).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(20).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(20).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(20).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build()
        ))
        .build();


    Map<String, CastCheckResult> otherTestResults = new HashMap<>();
    otherTestResults.put(
        CheckNames.EN_BACKGROUND_CHECK.getName(),
        CastCheckResult.builder().withCastNumber(1).withPassed(false).withFailedDepths(Arrays.asList(0, 2, 3)).build());

    EnBackgroundCheckerResult bgResult = new EnBackgroundCheckerResult();
    bgResult.setFailures(new HashSet<>(Arrays.asList(1)));
    bgResult.getLevels().add(new EnBackgroundCheckerLevelResult(0, 20, 10));
    bgResult.getLevels().add(new EnBackgroundCheckerLevelResult(2, 20, 9));
    bgResult.getLevels().add(new EnBackgroundCheckerLevelResult(3, 20, 8));
    bgResult.getLevels().add(new EnBackgroundCheckerLevelResult(4, 20, 7));

    EnBackgroundChecker enBackgroundChecker = mock(EnBackgroundChecker.class);
    when(enBackgroundChecker.getFailedDepths(cast, otherTestResults)).thenReturn(bgResult);

    StdLevelResolver stdLevelResolver = new StdLevelResolver(enBackgroundChecker, slev);
    List<StdLevel> stdLevels = stdLevelResolver.getStdLevels(cast, otherTestResults);

    assertEquals(1, stdLevels.size());
    assertEquals(4, stdLevels.get(0).getLevelWrappers().get(0).getLevel().getOrigLevel());
    assertEquals(13D, stdLevels.get(0).getStdLevel(), 0.001);


  }

}