package edu.colorado.cires.wod.iquodqc.check.argo.spike;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

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

public class ArgoSpikeCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.ARGO_SPIKE_CHECK.getName();
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new TreeSet<>();
    double latitude = cast.getLatitude();

    for (int i = 1; i < depths.size()-1; i++) {
      Optional<Double> prevTemp= getTemperature(depths.get(i-1)).map(v -> v.getValue());
      Optional<Double> currentTemp= getTemperature(depths.get(i)).map(v -> v.getValue());
      Optional<Double> nextTemp= getTemperature(depths.get(i+1)).map(v -> v.getValue());
      if (prevTemp.isPresent() && currentTemp.isPresent() && nextTemp.isPresent()) {
        double pressure = ObsUtils.depthToPressure(depths.get(i).getDepth(),latitude);
        double prevT = prevTemp.get();
        double nextT = nextTemp.get();
        double spike = Math.abs(currentTemp.get() - (prevT + nextT)/2 - Math.abs((nextT - prevT)/2));
        if (pressure < 500){
          if (spike > 6.0){
            failures.add(i);
          }
        } else {
          if (spike > 2.0){
            failures.add(i);
          }
        }
      }
    }

    return failures;
  }
}
