package edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.List;

public class CoTeDeTukey53HCheck extends CommonCastCheck {
  
  private static final double TEMPERATURE_THRESHOLD = 2.5;

  @Override
  public String getName() {
    return CheckNames.COTEDE_TUKEY_53H_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    return checkTukey53H(
        cast.getDepths().stream()
            .map(d ->
                getTemperature(d)
                    .map(ProfileData::getValue)
                    .orElse(Double.NaN)
            ).mapToDouble(Double::doubleValue)
            .toArray(),
        getTemperatureThreshold()
    );
  }
  
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
  
  protected List<Integer> checkTukey53H(double[] input, double threshold) {
    return CoTeDeTukey53H.checkTukey53H(
        input,
        threshold,
        false
    );
  }
}
