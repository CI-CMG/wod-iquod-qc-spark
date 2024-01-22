package edu.colorado.cires.wod.iquodqc.common;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants.XBT;

import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Optional;
import java.util.OptionalInt;

public final class CastUtils {

  public static Optional<Attribute> getProbeType(Cast cast) {
    return cast.getAttributes().stream().filter(a -> a.getCode() == PROBE_TYPE).findFirst();
  }

  public static OptionalInt getProbeTypeValue(Cast cast) {
    return getProbeType(cast).map(Attribute::getValue).stream().mapToInt(Double::intValue).findFirst();
  }

  public static boolean isProbeTypeXBT(Cast cast) {
    return getProbeTypeValue(cast).equals(OptionalInt.of(XBT));
  }

  public static double[] getTemperatures(Cast cast) {
    return cast.getDepths().stream()
        .map(depth ->
            getTemperature(depth)
                .map(ProfileData::getValue)
                .orElse(Double.NaN)
        ).mapToDouble(Double::doubleValue)
        .toArray();
  }

  public static double[] getDepths(Cast cast) {
    return cast.getDepths().stream()
        .map(Depth::getDepth)
        .mapToDouble(Double::doubleValue)
        .toArray();
  }

  private CastUtils() {

  }
}
