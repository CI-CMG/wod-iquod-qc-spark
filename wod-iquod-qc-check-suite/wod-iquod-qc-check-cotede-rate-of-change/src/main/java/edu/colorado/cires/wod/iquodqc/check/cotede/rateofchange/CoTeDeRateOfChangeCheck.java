package edu.colorado.cires.wod.iquodqc.check.cotede.rateofchange;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;

public class CoTeDeRateOfChangeCheck extends CommonCastCheck {
  
  private static final double TEMPERATURE_THRESHOLD = 4;
  
  @Override
  public String getName() {
    return "COTEDE_RATE_OF_CHANGE";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    
    return CoTeDeRateOfChange.checkRateOfChange(
        cast.getDepths().stream()
            .map(d ->
                getTemperature(d)
                    .map(ProfileData::getValue)
                    .orElse(Double.NaN)
            ).mapToDouble(Double::doubleValue)
            .toArray(),
        TEMPERATURE_THRESHOLD
    );
  }
}
