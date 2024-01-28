package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getTemperatures;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.SignalProducingCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CoTeDeSpikeCheck extends SignalProducingCastCheck {

  private static final double TEMPERATURE_THRESHOLD = 6;

  @Override
  public String getName() {
    return CheckNames.COTEDE_SPIKE_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    double[] temperatures = getTemperatures(cast);
    double[] spikes = CoTeDeSpike.computeSpikes(temperatures);
    signal = Arrays.stream(spikes).boxed().collect(Collectors.toList());
    return CoTeDeSpike.getFlags(
        getTemperatures(cast),
        spikes,
        getTemperatureThreshold()
    );
  }

  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
}
