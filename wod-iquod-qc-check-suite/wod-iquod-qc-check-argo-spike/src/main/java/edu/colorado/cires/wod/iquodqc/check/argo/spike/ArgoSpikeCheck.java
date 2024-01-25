package edu.colorado.cires.wod.iquodqc.check.argo.spike;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.ObsUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ArgoSpikeCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.ARGO_SPIKE_CHECK.getName();
  }

  private static double getPressure(Map<Integer, Double> pressureCache, int depthIndex, double depth, double latitude) {
    Double pressure = pressureCache.get(depthIndex);
    if (pressure != null) {
      return pressure;
    }
    pressure = ObsUtils.depthToPressure(depth, latitude);
    pressureCache.put(depthIndex, pressure);
    return pressure;
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new TreeSet<>();
    double latitude = cast.getLatitude();

    Map<Integer, Double> pressureCache = new HashMap<>();

    for (int i = 1; i < depths.size() - 1; i++) {
      final int depthIndex = i;
      final double depth = depths.get(i).getDepth();
      getTemperature(depths.get(depthIndex - 1)).map(ProfileData::getValue).ifPresent(prevT -> {
        getTemperature(depths.get(depthIndex)).map(ProfileData::getValue).ifPresent(currentT -> {
          getTemperature(depths.get(depthIndex + 1)).map(ProfileData::getValue).ifPresent(nextT -> {
            double pressure = getPressure(pressureCache, depthIndex, depth, latitude);
            double spike = Math.abs(currentT - (prevT + nextT) / 2D) - Math.abs((nextT - prevT) / 2D);
            if (pressure < 500D) {
              if (spike > 6D) {
                failures.add(depthIndex);
              }
            } else {
              if (spike > 2D) {
                failures.add(depthIndex);
              }
            }
          });
        });
      });
    }

    return failures;
  }
}
