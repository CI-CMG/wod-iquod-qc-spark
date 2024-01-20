package edu.colorado.cires.wod.iquodqc.check.argo.pressureincreasing;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static edu.colorado.cires.wod.iquodqc.common.DoubleUtils.doubleEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.ObsUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class ArgoPressureIncreasingCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.ARGO_PRESSURE_INCREASING_TEST.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new TreeSet<>();
    double latitude = cast.getLatitude();

    int iRef = 0;
    double zRef = ObsUtils.depthToPressure(depths.get(0).getDepth(),latitude);
    for (int i = 1; i < depths.size(); i++) {
      double pressure = ObsUtils.depthToPressure(depths.get(i).getDepth(),latitude);
//       Check for non-increasing pressure. If pressure increases, update the reference.
      if (doubleEquals(Optional.of(pressure), Optional.of(zRef))){
        failures.add(i);
      } else if (pressure < zRef) {
        failures.add(iRef);
        failures.add(i);
      } else {
        iRef = i;
        zRef = pressure;
      }
    }
    return failures;
  }
}
