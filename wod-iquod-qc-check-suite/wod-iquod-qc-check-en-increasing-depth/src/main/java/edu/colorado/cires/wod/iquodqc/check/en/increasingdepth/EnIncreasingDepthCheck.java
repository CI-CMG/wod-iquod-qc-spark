package edu.colorado.cires.wod.iquodqc.check.en.increasingdepth;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class EnIncreasingDepthCheck extends CommonCastCheck {

  private static final String NAME = "EN_increasing_depth_check";
  private final static String EN_SPIKE_AND_STEP_CHECK = "EN_spike_and_step_check";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> dependsOn() {
    return Collections.singleton(EN_SPIKE_AND_STEP_CHECK);
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    throw new UnsupportedOperationException("This method is not used");
  }

  private static void maskIndex(boolean[][] inconsistencies, int index) {
    for (int i = 0; i < inconsistencies.length; i++) {
      inconsistencies[index][i] = false;
      inconsistencies[i][index] = false;
    }
  }

  private static void failAll(Set<Integer> failures, Cast cast) {
    for (int i = 0; i < cast.getDepths().size(); i++) {
      failures.add(i);
    }
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {

    Set<Integer> failures = new TreeSet<>();

    // Basic check on each level.
    for (int i = 0; i < cast.getDepths().size(); i++) {
      double depth = cast.getDepths().get(i).getDepth();
      if (depth < 0D || depth > 11000) {
        failures.add(i);
      }
    }

    // don't perform more sophisticated tests for single-level profiles
    if (cast.getDepths().size() > 1) {
      DepthCounter depthCounter = new DepthCounter();
      cast.getDepths().stream().mapToDouble(Depth::getDepth).forEach(depthCounter::increment);
      // if all the depths are the same, flag all levels and finish immediately
      if (depthCounter.getCounts().size() == 1) {
        failAll(failures, cast);
      } else {
        boolean[][] inconsistencies = new boolean[cast.getDepths().size()][cast.getDepths().size()];
        for (int i = 0; i < cast.getDepths().size(); i++) {
          double depth = cast.getDepths().get(i).getDepth();
          for (int j = 0; j < cast.getDepths().size(); j++) {
            double checkDepth = cast.getDepths().get(j).getDepth();
            boolean consistent = depth > checkDepth;
            if (i != j) {
              inconsistencies[i][j] = i < j ? consistent : !consistent;
            }
          }
        }

        int currentMax = 1;
        int currentLev = -1;
        int otherLev = -1;
        while (currentMax > 0) {
          currentMax = 0;
          currentLev = -1;
          otherLev = -1;
          for (int i = 0; i < cast.getDepths().size(); i++) {
            int lineSum = 0;
            for (int j = 0; j < cast.getDepths().size(); j++) {
              if (inconsistencies[j][i]) {
                lineSum++;
              }
            }
            if (lineSum >= currentMax) {
              currentMax = lineSum;
              currentLev = i;
            }
          }
          // Reject immediately if more than one inconsistency or
          // investigate further if one inconsistency.
          if (currentMax > 1) {
            failures.add(currentLev);
          } else if (currentMax == 1) {

            // Find out which level it is inconsistent with.
            for (int i = 0; i < cast.getDepths().size(); i++) {
              if (inconsistencies[i][currentLev]) {
                otherLev = i;
              }
            }

            // Check if one was rejected by the spike and step
            // check, otherwise reject both.
            CastCheckResult spikeqc = otherTestResults.get(EN_SPIKE_AND_STEP_CHECK);
            if (spikeqc.getFailedDepths().contains(currentLev)) {
              failures.add(currentLev);
            }
            if (spikeqc.getFailedDepths().contains(otherLev)) {
              failures.add(otherLev);
            }
            if (!spikeqc.getFailedDepths().contains(currentLev) && !spikeqc.getFailedDepths().contains(otherLev)) {
              failures.add(currentLev);
              failures.add(otherLev);
            }
          }

          if (currentLev > -1 && failures.contains(currentLev)) {
            maskIndex(inconsistencies, currentLev);
          }
          if (otherLev > -1 && failures.contains(otherLev)) {
            maskIndex(inconsistencies, otherLev);
          }
        }
      }
    }

    return failures;
  }

}
