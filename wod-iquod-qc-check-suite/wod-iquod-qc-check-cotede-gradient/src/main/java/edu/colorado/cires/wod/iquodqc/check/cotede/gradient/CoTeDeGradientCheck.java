package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;

public class CoTeDeGradientCheck extends CommonCastCheck {
  
  private static final double TEMPERATURE_THRESHOLD = 1.5;

  @Override
  public String getName() {
    return "COTEDE_GRADIENT_CHECK";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    return CoTeDeGradient.checkGradient(
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
}
