package edu.colorado.cires.wod.iquodqc.check.aoml.gross;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class AomlGrossCheck extends CommonCastCheck {

  private static final double MAX_DEPTH = 2000D; //TODO get from properties
  private static final double MIN_TEMP = -2.5D; //TODO get from properties
  private static final double MAX_TEMP = 40D; //TODO get from properties

  @Override
  public String getName() {
    return "AOML_gross";
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);

      //TODO can this ever be null?
//      if (depth.getDepth() != null) {
      boolean failed = depth.getDepth() < 0 || depth.getDepth() > MAX_DEPTH;

      if (!failed) {
        failed = DepthUtils.getTemperature(depth)
            .map(ProfileData::getValue)
            .map(temp -> temp < MIN_TEMP || temp > MAX_TEMP)
            .orElse(false);
      }

      if (failed) {
        failures.add(i);
      }

    }

    return failures;
  }
}
