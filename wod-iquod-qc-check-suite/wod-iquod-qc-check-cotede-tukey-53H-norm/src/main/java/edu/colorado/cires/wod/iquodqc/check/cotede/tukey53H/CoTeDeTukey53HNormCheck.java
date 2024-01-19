package edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H;

import static edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H.CoTeDeTukey53H.computeTukey53H;
import static edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H.CoTeDeTukey53H.getFlags;
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

public class CoTeDeTukey53HNormCheck extends SignalProducingCastCheck {

  private static final double TEMPERATURE_THRESHOLD = 2.5;

  @Override
  public String getName() {
    return CheckNames.COTEDE_TUKEY_53_NORM_CHECK.getName();
  }

  @Override
  protected List<Double> produceSignal(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    return Arrays.stream(computeTukey53H(
        getTemperatures(cast),
        true
    )).boxed().collect(Collectors.toList());
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, List<Double> signal, Map<String, CastCheckResult> otherTestResults) {
    return getFlags(
        getTemperatures(cast),
        signal.stream()
            .mapToDouble(Double::doubleValue)
            .toArray(),
        TEMPERATURE_THRESHOLD
    );
  }

}
