package edu.colorado.cires.wod.iquodqc.check.argo.densityinversion;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.argo.densityinversion.DensityInversion.InsituDensityComputation;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class DensityInversionCheck extends CommonCastCheck {
  
  private static final double THRESHOLD = -0.03;

  @Override
  public String getName() {
    return CheckNames.COTEDE_ARGO_DENSITY_INVERSION_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    InsituDensityComputation computation = null;
    List<Integer> failures = new ArrayList<>(0);
    
    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      
      computation = DensityInversion.computeInsituDensity(
          getValue(DepthUtils::getSalinity, depth),
          getValue(DepthUtils::getTemperature, depth),
          getValue(DepthUtils::getPressure, depth),
          computation
      );
      
      double difference = computation.getDifference();
      
      if (computation.getDifference() < THRESHOLD) {
        failures.add(i);
      }
    }
    
    return failures;
  }
  
  @FunctionalInterface
  private interface GetProfileData {
    Optional<ProfileData> getValue(Depth depth);
  } 
  
  private static double getValue(GetProfileData method, Depth depth) {
    return method.getValue(depth).map(ProfileData::getValue)
        .orElse(Double.NaN);
  }
}
