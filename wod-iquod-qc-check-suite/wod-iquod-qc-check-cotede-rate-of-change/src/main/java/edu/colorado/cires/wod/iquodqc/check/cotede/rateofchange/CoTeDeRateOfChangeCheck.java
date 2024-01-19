package edu.colorado.cires.wod.iquodqc.check.cotede.rateofchange;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getTemperatures;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.SignalProducingCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CoTeDeRateOfChangeCheck extends SignalProducingCastCheck {

  private static final double TEMPERATURE_THRESHOLD = 4;

  @Override
  public String getName() {
    return CheckNames.COTEDE_RATE_OF_CHANGE.getName();
  }

  @Override
  protected List<Double> produceSignal(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    return Arrays.stream(CoTeDeRateOfChange.computeRateOfChange(getTemperatures(cast)))
        .boxed().collect(Collectors.toList());
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, List<Double> signal, Map<String, CastCheckResult> otherTestResults) {
    return CoTeDeRateOfChange.getFlags(
        getTemperatures(cast),
        signal.stream()
            .mapToDouble(Double::doubleValue)
            .toArray(),
        TEMPERATURE_THRESHOLD
    );
  }
}
