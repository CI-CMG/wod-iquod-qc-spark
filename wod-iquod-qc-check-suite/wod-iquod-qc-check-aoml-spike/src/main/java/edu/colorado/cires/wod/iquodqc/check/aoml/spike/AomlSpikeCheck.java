package edu.colorado.cires.wod.iquodqc.check.aoml.spike;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.ArrayUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.math3.util.Precision;

public class AomlSpikeCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return "AOML_spike";
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new TreeSet<>();

    if (depths.size() < 3) {
      return failures;
    }

    for (int i = 2; i < depths.size() - 2; i++) {
      boolean failure = spike(depths.subList(i - 2, i + 3));
      if (failure) {
        failures.add(i);
      }
    }

    boolean f1 = spike(depths.subList(0, 3));
    if (f1) {
      failures.add(1);
    }

    boolean f2 = spike(depths.subList(depths.size() - 3, depths.size()));
    if (f2) {
      failures.add(depths.size() - 2);
    }

    return failures;
  }


  private static double round(double number, int decimalsToConsider) {
    return new BigDecimal(number).setScale(decimalsToConsider, RoundingMode.HALF_UP).doubleValue();
  }


  private boolean spike(List<Depth> depths) {
    double[] t = new double[depths.size()];
    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      if (getTemperature(depth).isEmpty()) {
        return false;
      }
      t[i] = getTemperature(depth).get().getValue();
    }
    double centralTemp = t[t.length / 2];
    double medianDiff = round(Math.abs(centralTemp - ArrayUtils.median(t)), 2);

    if (!Precision.equals(0D, medianDiff)) {
      t = ArrayUtils.deleteElement(t, t.length / 2);
      double spikeCheck = round(Math.abs(centralTemp - ArrayUtils.mean(t)), 2);
      if (spikeCheck > 0.3) {
        return true;
      }

    }

    return false;
  }
}
