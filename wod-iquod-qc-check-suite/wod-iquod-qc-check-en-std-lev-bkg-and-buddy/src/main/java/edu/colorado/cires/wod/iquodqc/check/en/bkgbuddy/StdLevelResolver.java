package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundChecker;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundCheckerLevelResult;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundCheckerResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

class StdLevelResolver {

  static final Collection<String> OTHER_TESTS = Collections.unmodifiableList(Arrays.asList(
      CheckNames.EN_BACKGROUND_CHECK.getName(),
      CheckNames.EN_CONSTANT_VALUE_CHECK.getName(),
      CheckNames.EN_INCREASING_DEPTH_CHECK.getName(),
      CheckNames.EN_RANGE_CHECK.getName(),
      CheckNames.EN_SPIKE_AND_STEP_CHECK.getName(),
      CheckNames.EN_STABILITY_CHECK.getName()
  ));

  private final EnBackgroundChecker enBackgroundChecker;
  private final List<Double> slev;

  StdLevelResolver(EnBackgroundChecker enBackgroundChecker, List<Double> slev) {
    this.enBackgroundChecker = enBackgroundChecker;
    this.slev = slev;
  }

  List<StdLevel> getStdLevels(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    final Set<Integer> preQc = new HashSet<>();
    for (Entry<String, CastCheckResult> entry : otherTestResults.entrySet()) {
      if (OTHER_TESTS.contains(entry.getKey())) {
        preQc.addAll(entry.getValue().getFailedDepths());
      }
    }

    Map<Integer, StdLevel> stdLevelMap = new TreeMap<>();

    EnBackgroundCheckerResult bgResult = enBackgroundChecker.getFailedDepths(cast, otherTestResults);
    for (EnBackgroundCheckerLevelResult levelResult : bgResult.getLevels()) {
      if (!preQc.contains(levelResult.getOrigLevel())) {
        StdLevelWrapper stdLevelWrapper = atStandardLevel(cast.getDepths(), levelResult, slev);
        StdLevel stdLevel = stdLevelMap.get(stdLevelWrapper.getStdLevelIndex());
        if (stdLevel == null) {
          stdLevel = new StdLevel(stdLevelWrapper.getStdLevelIndex());
          stdLevelMap.put(stdLevelWrapper.getStdLevelIndex(), stdLevel);
        }
        stdLevel.getLevelWrappers().add(stdLevelWrapper);
      }
    }

    stdLevelMap.values().forEach(StdLevel::finalizeStdLevel);

    return new ArrayList<>(stdLevelMap.values());

  }

  private static StdLevelWrapper atStandardLevel(List<Depth> depths, EnBackgroundCheckerLevelResult level, List<Double> slev) {
    double diffLevel = level.getPtLevel() - level.getBgLevel();
    // Find the closest standard level
    int minIndex = -1;
    double minDiff = Integer.MAX_VALUE;
    for (int j = 0; j < slev.size(); j++) {
      Double depth = slev.get(j);
      if (depth != null) {
        double diff = Math.abs(depths.get(level.getOrigLevel()).getDepth() - depth);
        if (minIndex < 0 || diff < minDiff) {
          minIndex = j;
          minDiff = diff;
        }
      }
    }
    return new StdLevelWrapper(level, minIndex, minDiff, diffLevel);
  }


}
