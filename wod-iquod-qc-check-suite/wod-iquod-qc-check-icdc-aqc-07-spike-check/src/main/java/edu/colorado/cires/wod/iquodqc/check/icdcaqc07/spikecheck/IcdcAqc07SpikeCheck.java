package edu.colorado.cires.wod.iquodqc.check.icdcaqc07.spikecheck;

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

public class IcdcAqc07SpikeCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.ICDC_AQC_07_SPIKE_CHECK.getName();
  }
/*
   # The test is run on re-ordered data.
    nlevels, z, t = ICDC.reordered_data(p, data_store)
    qc = np.zeros(nlevels, dtype=bool) # Reordered data may be a subset of available levels.
    defaultqc = np.zeros(p.n_levels(), dtype=bool) # Default QC flags for full set of levels.
    if nlevels < 3: return defaultqc # Not enough levels to check.

    # Ignore any levels outside of limits.
    parminover = -2.3
    parmaxover = 33.0
    use = (t > parminover) & (t < parmaxover)
    nuse = np.count_nonzero(use)
    if nuse < 3: return defaultqc
    zuse = z[use]
    tuse = t[use]
    origlevels = (np.arange(nlevels))[use]

    # Extract sections of the arrays. We are QCing the values
    # in the z2 and v3 arrays.
    z1 = zuse[0:-2]
    z2 = zuse[1:-1]
    z3 = zuse[2:]
    v1 = tuse[0:-2]
    v2 = tuse[1:-1]
    v3 = tuse[2:]
    ol = origlevels[1:-1]

    # Calculate the level of 'spike'.
    z13 = z3 - z1
    z12 = z2 - z1
    z23 = z3 - z2

    a  = 0.5 * (v1 + v3)
    q1 = np.abs(v2 - a)
    q2 = np.abs(0.5 * (v3 - v1))

    spike = q1 - q2

    # Define the threshold at each level.
    spikemax = np.ndarray(nuse - 2)
    spikemax[:]           = 4.0
    spikemax[z2 > 1000.0] = 3.0
    spikemax[z2 > 2000.0] = 2.0

    # Set QC flags.
    qc[ol[spike > spikemax]] = True

    return ICDC.revert_qc_order(p, qc, data_store)


 */

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    Set<Integer> failures = new LinkedHashSet<>();
    DepthData depthData = new DepthData(cast);

    //Check that we have the levels we need.
    if (depthData.getnLevels() <3) {
      return failures;
    }

//     Ignore any levels outside of limits.
    List<Integer> origlevels = depthData.getOriglevels();
    List<Double> depths = depthData.getDepths();
    List<Double> temperatures = depthData.getTemperatures();
    double parminover = -2.3;
    double parmaxover = 33.0;

//    int[] index = IntStream.range(0, temperatures.size())
//        .filter(i -> temperatures.get(i) > parminover && temperatures.get(i) < parmaxover)
//        .toArray();
//    if(index.length < 3){
//      return failures;
//    }

    List <Double> z = new ArrayList<>();
    List <Double> t = new ArrayList<>();
    List<Integer> uselevels = new ArrayList<>();

    for (int i = 0; i < temperatures.size(); i++){
      if (temperatures.get(i) > parminover && temperatures.get(i) < parmaxover){
        t.add(temperatures.get(i));
        z.add(depths.get(i));
        uselevels.add(origlevels.get(i));
      }
    }

    // Calculate the level of 'spike'.
    for (int i= 1; i< t.size()-2; i++) {
      double z2 = z.get(i);
      double v1 = t.get(i-1);
      double v2 = t.get(i);
      double v3 = t.get(i+1);
      double a = 0.5 * (v1+v3);
      double q1 = Math.abs(v2 - a);
      double q2 = Math.abs(0.5 * (v3-v1));
      double spike = q1-q2;
      if (spike > spikeMax(z2)){
        failures.add(uselevels.get(i));
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
