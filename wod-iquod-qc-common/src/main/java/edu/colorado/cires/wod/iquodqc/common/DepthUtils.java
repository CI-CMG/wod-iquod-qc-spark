package edu.colorado.cires.wod.iquodqc.common;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.SALINITY;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getProbeTypeValue;
import static edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants.XBT;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Optional;
import java.util.OptionalInt;

public final class DepthUtils {

  public static Optional<ProfileData> getTemperature(Depth depth) {
    return depth.getData().stream().filter((pd) -> pd.getVariable() == TEMPERATURE).findFirst();
  }

  public static Optional<ProfileData> getSalinity(Depth depth) {
    return depth.getData().stream().filter((pd) -> pd.getVariable() == SALINITY).findFirst();
  }

  public static Optional<ProfileData> getPressure(Depth depth) {
    return depth.getData().stream().filter((pd) -> pd.getVariable() == PRESSURE).findFirst();
  }

//  public static boolean isProbeTypeXBT(Cast cast) {
////    Optional<Double> probeType = CastUtils.getProbeType(cast).map(Attribute::getValue);
//    return getProbeTypeValue(cast).equals(OptionalInt.of(XBT));
//  }

  private DepthUtils() {

  }

}
