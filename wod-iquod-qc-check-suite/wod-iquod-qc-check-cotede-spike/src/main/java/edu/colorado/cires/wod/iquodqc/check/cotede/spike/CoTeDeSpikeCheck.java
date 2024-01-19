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

  private static final double TEMPERATURE_THRESHOLD = 4;

  @Override
  public String getName() {
    return CheckNames.COTEDE_SPIKE_CHECK.getName();
  }

  @Override
  protected List<Double> produceSignal(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    return Arrays.stream(CoTeDeSpike.computeSpikes(getTemperatures(cast)))
        .boxed().collect(Collectors.toList());
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, List<Double> signal, Map<String, CastCheckResult> otherTestResults) {
    return CoTeDeSpike.getFlags(
        getTemperatures(cast),
        signal.stream()
            .mapToDouble(Double::doubleValue)
            .toArray(),
        getTemperatureThreshold()
    );
  }

  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
}
