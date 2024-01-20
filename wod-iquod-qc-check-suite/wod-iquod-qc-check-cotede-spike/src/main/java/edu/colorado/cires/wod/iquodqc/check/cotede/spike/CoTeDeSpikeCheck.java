package edu.colorado.cires.wod.iquodqc.check.cotede.spike;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;

public class CoTeDeSpikeCheck extends CommonCastCheck {

  private static final double TEMPERATURE_THRESHOLD = 4;

  @Override
  public String getName() {
    return CheckNames.COTEDE_SPIKE_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    return CoTeDeSpike.checkSpike(
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
