package edu.colorado.cires.wod.iquodqc.check.icdcaqc08.gradientcheck;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getProbeTypeValue;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants;
import edu.colorado.cires.wod.iquodqc.common.icdc.DepthData;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IcdcAqc08GradientCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.ICDC_AQC_08_GRADIENT_CHECK.getName();
  }
/*
   # Global ranges - data outside these bounds are ignored.
    parminover = -2.3
    parmaxover = 33.0

    # The test is run on re-ordered data.
    nlevels, z, t = ICDC.reordered_data(p, data_store)
    qc = np.zeros(nlevels, dtype=bool)

    # Calculate gradients and thresholds.
    z0 = z[0:-1]
    z1 = z[1:]
    t0 = t[0:-1]
    t1 = t[1:]

    gradients = (t1 - t0) / (z1 - z0)
    zmean     = 0.5 * (z0 + z1)
    zmean[zmean < 1.0] = 1.0

    gradmin = -150.0 / zmean - 0.010
    gradmin[gradmin < -4.0] = -4.0

    gradmax = 100.0 / zmean + 0.015
    gradmax[gradmax > 1.5] = 1.5

    # Find where the gradients and outside the thresholds.
    result = np.where(((gradients < gradmin) | (gradients > gradmax)) &
                       (t0 > parminover) & (t1 > parminover) &
                       (t0 < parmaxover) & (t1 < parmaxover))[0]

    # Both levels that form the gradient have to be rejected.
    if len(result) > 0:
        qc[result] = True
        qc[result + 1] = True

    return ICDC.revert_qc_order(p, qc, data_store)


 */

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    Set<Integer> failures = new LinkedHashSet<>();
    DepthData depthData = new DepthData(cast);
    List<Integer> origlevels = depthData.getOriglevels();
    List<Double> depths = depthData.getDepths();
    List<Double> temperatures = depthData.getTemperatures();

    //# Global ranges - data outside these bounds are ignored.
    double parminover = -2.3;
    double parmaxover = 33.0;

    List<Integer> results = new ArrayList<>();

    for (int i = 0; i < temperatures.size()-1; i++){
      double z0 = depths.get(i);
      double z1 = depths.get(i+1);
      double t0 = temperatures.get(i);
      double t1 = temperatures.get(i+1);
      double gradient = (t1-t0)/(z1-z0);
      double zmean = 0.5 * (z0+z1);
      double gradmin = -150.0 / zmean - 0.010;
      if (gradmin < -4.0){
        gradmin = -4.0;
      }

      double gradmax = 100.0 / zmean + 0.015;
      if (gradmax > 1.5){
        gradmax = 1.5;
      }

      //# Find where the gradients and outside the thresholds.
      if (((gradient < gradmin) || (gradient > gradmax)) && (t0 > parminover) && (t1 > parminover) &
          (t0 < parmaxover) && (t1 < parmaxover)){
        failures.add(origlevels.get(i));
        failures.add(origlevels.get(i+1));
      }
    }

    return failures;
  }

  private double spikeMax(double z2){
    if (z2 > 2000.0) {
      return 2.0;
    } else if (z2 > 1000.0) {
      return 3.0;
    }
    return 4.0;
  }
}
