package edu.colorado.cires.wod.iquodqc.common;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;

import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Optional;

public final class CastUtils {

  public static Optional<Attribute> getProbeType(Cast cast) {
    return cast.getAttributes().stream().filter(a -> a.getCode() == PROBE_TYPE).findFirst();
  }

  private CastUtils() {

  }
}
