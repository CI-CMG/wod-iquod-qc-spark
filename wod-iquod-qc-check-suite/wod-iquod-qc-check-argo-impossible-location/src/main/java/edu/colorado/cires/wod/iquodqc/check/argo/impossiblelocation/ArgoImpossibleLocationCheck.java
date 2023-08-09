package edu.colorado.cires.wod.iquodqc.check.argo.impossiblelocation;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ArgoImpossibleLocationCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return "Argo_impossible_location_test";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    if (cast.getLatitude() < -90D || cast.getLatitude() > 90D || cast.getLongitude() < -180D || cast.getLongitude() > 180D) {
      for (int i = 0; i < depths.size(); i++) {
        failures.add(i);
      }
    }
    return failures;
  }

}
