package edu.colorado.cires.wod.iquodqc.common.en;

import edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants;
import java.util.Set;

public final class PgeEstimator {

  private static final Set<Integer> PGE_TYPES = Set.of(
      ProbeTypeConstants.MBT,
      ProbeTypeConstants.XBT,
      ProbeTypeConstants.DBT,
      ProbeTypeConstants.ANIMAL_MOUNTED,
      ProbeTypeConstants.MICRO_BT
  );

  public static double estimateProbabilityOfGrossError(int probeType, boolean failedSpikeTest) {
    //  Estimates the probability of gross error for a measurement taken by
    //  the given probe_type. Information from the EN_spike_and_step_check
    //  is used here to increase the initial estimate if the observation is suspect.
    double pge = 0.01;
    if (PGE_TYPES.contains(probeType)) {
      pge = 0.05;
    }
    if (failedSpikeTest) {
      pge = 0.5 + 0.5 * pge;
    }
    return pge;
  }

  private PgeEstimator() {

  }

}
