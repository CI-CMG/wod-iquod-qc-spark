package edu.colorado.cires.wod.iquodqc.common.en;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class EnSpikeAndStepInitializer {

  private final List<Double> t;
  private final double[] z;
  private final List<Double> dt;
  private final List<Double> gt;

  EnSpikeAndStepInitializer(Cast cast) {
    List<Depth> depths = cast.getDepths();
    t = cast.getDepths().stream().map(d -> getTemperature(d).map(ProfileData::getValue).orElse(null)).collect(Collectors.toList());
    z = cast.getDepths().stream().mapToDouble(Depth::getDepth).toArray();
    dt = new ArrayList<>(depths.size());
    gt = new ArrayList<>(depths.size());
    for (int i = 0; i < depths.size(); i++) {
      dt.add(0D);
      gt.add(0D);
    }
    composeDT(t, z, depths.size(), dt, gt);
  }

  private static void composeDT(List<Double> var, double[] z, int nLevels, List<Double> dt, List<Double> gt) {
    // build the array of deltas for the variable provided

    for (int i = 1; i < nLevels; i++) {
      if ((z[i] - z[i - 1]) <= 50.0 || (z[i] >= 350.0 && (z[i] - z[i - 1]) <= 100.0)) {
        if (var.get(i) != null && var.get(i - 1) != null) {
          dt.set(i, var.get(i) - var.get(i - 1));
        }
        gt.set(i, dt.get(i) / Math.max(10.0, z[i] - z[i - 1]));
      }
    }

  }

  List<Double> getT() {
    return t;
  }

  double[] getZ() {
    return z;
  }

  List<Double> getDt() {
    return dt;
  }

  List<Double> getGt() {
    return gt;
  }
}
