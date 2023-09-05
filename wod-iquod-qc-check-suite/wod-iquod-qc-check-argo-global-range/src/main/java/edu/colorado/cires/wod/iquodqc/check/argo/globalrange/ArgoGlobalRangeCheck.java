package edu.colorado.cires.wod.iquodqc.check.argo.globalrange;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static java.lang.Math.max;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.ObsUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ArgoGlobalRangeCheck extends CommonCastCheck {
  @Override
  public String getName() {
    return "Argo_global_range_check";
  }


  /*
  Runs the quality control check on profile p and returns a numpy array
      of quality control decisions with False where the data value has
      passed the check and True where it failed.
      """

      # Get temperature and pressure values from the profile.
      t = p.t()
      z = obs_utils.depth_to_pressure(p.z(), p.latitude())

      # Make the quality control decisions. This should
      # return true if the temperature is outside -2.5 deg C
      # and 40 deg C or pressure is less than -5.
      qct = (t.mask == False) & ((t.data < -2.5) | (t.data > 40.0))
      qcp = (z.mask == False) & (z.data < -5)
      qc  = qct | qcp

      return qc
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    double latitude = cast.getLatitude();

    for (int i = 0; i < depths.size(); i++) {
      double pressure = ObsUtils.depthToPressure(depths.get(i).getDepth(),latitude);
      if (pressure < -5D){
        failures.add(i);
        continue;
      }
      Optional<Double> temperature= getTemperature(depths.get(i)).map(v -> v.getValue());
      if (temperature.isPresent()) {
        double temp = temperature.get();
        if ((temp < -2.5) || (temp > 40.0)) {
          failures.add(i);
        }
      }
    }
    return failures;
  }
}
