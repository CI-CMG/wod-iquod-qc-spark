package edu.colorado.cires.wod.iquodqc.common;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.XBT;
import static edu.colorado.cires.wod.iquodqc.common.DoubleUtils.doubleEquals;

import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Optional;
import org.apache.commons.math3.util.Precision;

public final class DepthUtils {

  public static Optional<ProfileData> getTemperature(Depth depth) {
    return depth.getData().stream().filter((pd) -> pd.getVariable() == TEMPERATURE).findFirst();
  }



  public static boolean isProbeTypeXBT(Cast cast){
    Optional<Double> probeType = cast.getAttributes().stream()
        .filter(a -> a.getCode() == PROBE_TYPE).findFirst().map(Attribute::getValue);
    return doubleEquals(probeType, Optional.of((double) XBT));
  }
  private DepthUtils () {

  }

}
