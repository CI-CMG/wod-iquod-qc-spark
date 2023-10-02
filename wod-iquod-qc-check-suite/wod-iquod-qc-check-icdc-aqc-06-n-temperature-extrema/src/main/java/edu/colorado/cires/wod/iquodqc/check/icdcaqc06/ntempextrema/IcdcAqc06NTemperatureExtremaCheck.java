package edu.colorado.cires.wod.iquodqc.check.icdcaqc06.ntempextrema;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getProbeTypeValue;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants;
import edu.colorado.cires.wod.iquodqc.common.icdc.DepthData;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IcdcAqc06NTemperatureExtremaCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return "ICDC_aqc_06_n_temperature_extrema";
  }
/*
     # Initialise data.
    qc = np.zeros(p.n_levels(), dtype=bool)
    parminover = -2.3
    parmaxover = 33.0
    levminext  = 6
    deltaext   = 0.5
    maxextre   = 4

    # Check that we have the levels we need.
    nlevels, z, t = ICDC.reordered_data(p, data_store)
    if nlevels <= levminext: return qc

    # Exclude data outside allowed range.
    use  = (t > parminover) & (t <= parmaxover)
    nuse = np.count_nonzero(use)
    if nuse < levminext: return qc
    z = z[use]
    t = t[use]

    # Find and count the extrema.
    ima = 0
    for i in range(1, nuse - 1):
        pcent = t[i]
        pa = np.abs(pcent - t[i - 1])
        pb = np.abs(pcent - t[i + 1])
        pmin = min(pa, pb)
        if pcent > t[i - 1] and pcent > t[i + 1] and pmin > deltaext:
            ima += 1
        if pcent < t[i - 1] and pcent < t[i + 1] and pmin > deltaext:
            ima += 1
    if ima > maxextre:
        qc[:] = True

    return qc


 */

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    Set<Integer> failures = new LinkedHashSet<>();
    int levminext  = 6;
    double deltaext   = 0.5;
    double maxextre   = 4;

    DepthData depthData = new DepthData(cast);

    //Check that we have the levels we need.
    if (depthData.getnLevels() <= levminext) {
      return failures;
    }

    List<Double> temperatures = depthData.getTemperatures();

    // Exclude data outside allowed range.
    List<Double> t = temperatures.stream()
        .filter(i -> i < 4 || i > 7)
        .collect(Collectors.toList());
    if(t.size() < levminext){
      return failures;
    }

    // Find and count the extrema.
    int ima = 0;
    for (int i = 1; i < t.size()-1; i++) {
      double pcent = t.get(i);
      double pa = Math.abs(pcent - t.get(i - 1));
      double pb = Math.abs(pcent - t.get(i + 1));
      double pmin = Math.min(pa, pb);
      if ((pcent > t.get(i - 1))&& (pcent > t.get(i + 1)) && (pmin > deltaext)){
        ima += 1;
      }

      if ((pcent < t.get(i - 1))&& (pcent < t.get(i + 1)) && (pmin > deltaext)){
        ima += 1;
      }
    }
    if (ima > maxextre){
      for(int i =0; i < cast.getDepths().size(); i++){
        failures.add(i);
      }
    }

    return failures;
  }

}
