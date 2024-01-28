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
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    double[] temperatures = getTemperatures(cast);
    double[] rateOfChange = CoTeDeRateOfChange.computeRateOfChange(temperatures);
    signal = Arrays.stream(rateOfChange).boxed().collect(Collectors.toList());
    return CoTeDeRateOfChange.getFlags(
        temperatures,
        rateOfChange,
        TEMPERATURE_THRESHOLD
    );
  }
}
