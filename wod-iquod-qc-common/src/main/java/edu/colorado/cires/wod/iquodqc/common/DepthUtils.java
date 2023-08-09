package edu.colorado.cires.wod.iquodqc.common;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;

import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Optional;

public final class DepthUtils {

  public static Optional<ProfileData> getTemperature(Depth depth) {
    return depth.getData().stream().filter((pd) -> pd.getVariable() == TEMPERATURE).findFirst();
  }

  private DepthUtils () {

  }

}
