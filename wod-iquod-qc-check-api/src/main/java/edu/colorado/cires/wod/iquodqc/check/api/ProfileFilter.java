package edu.colorado.cires.wod.iquodqc.check.api;

import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Optional;

public class ProfileFilter {
  private static final int TEMPERATURE = 1;
  private static final int Originator_Flags = 96;

  public static Optional<ProfileData> getTemperature(Depth depth) {
    return depth.getData().stream().filter((pd) -> pd.getVariableCode() == TEMPERATURE).findFirst();
  }

  public static String getFilterReason(Cast cast) {
    if (cast.getProfileType() == 1) {
      return "not interested in standard levels";
    }

    boolean hasTemp = cast.getDepths().stream().filter(depth -> getTemperature(depth).isPresent()).findFirst().isPresent();
    if (!hasTemp) {
      return "no temperature data in profile";
    }


    if (cast.getDepths().stream().allMatch(depth -> depth.getDepth() < 0.1) && cast.getDepths().size() > 1) {
      return "all depths are less than 10 cm and there are at least two levels (ie not just a surface measurement)";
    }

    int oFlag = cast.getAttributes().stream()
        .filter(attribute -> attribute.getCode() == Originator_Flags)
        .findFirst()
        .map(Attribute::getValue).orElse(-1D).intValue();
    if (oFlag < 1 || oFlag > 14) {
      return "no valid originator flag type";
    }

    if (cast.getMonth() < 1 || cast.getMonth() > 12) {
      return "invalid month";
    }

    return null;
  }

}
