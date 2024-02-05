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
import java.util.Map;
import java.util.stream.Collectors;

public class CoTeDeTukey53HNormCheck extends SignalProducingCastCheck {

  private static final double TEMPERATURE_THRESHOLD = 1.5;

  @Override
  public String getName() {
    return CheckNames.COTEDE_TUKEY_53_NORM_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    double[] temperatures = getTemperatures(cast);
    double[] tukeyNorm = computeTukey53H(temperatures, true);
    signal = Arrays.stream(tukeyNorm).boxed().collect(Collectors.toList());
    return getFlags(
        temperatures,
        tukeyNorm,
        TEMPERATURE_THRESHOLD
    );
  }

}
