package edu.colorado.cires.wod.iquodqc.check.cotede.globalrange;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;

public class CoTeDeGlobalRangeGTSPPCheck extends CommonCastCheck {
  
  private static final double TEMPERATURE_MIN = -2;
  private static final double TEMPERATURE_MAX = 40;

  @Override
  public String getName() {
    return "COTEDE_GLOBAL_RANGE_GTSPP_CHECK";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    return CoTeDeGlobalRange.checkGlobalRange(
        cast.getDepths().stream()
            .map(DepthUtils::getTemperature)
            .map(p -> p.map(ProfileData::getValue)
                .orElse(Double.NaN)
            ).mapToDouble(Double::doubleValue)
            .toArray(),
        TEMPERATURE_MIN,
        TEMPERATURE_MAX
    );
  }
}
