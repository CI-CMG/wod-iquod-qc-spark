package edu.colorado.cires.wod.iquodqc.check.wog.gradient;

import static edu.colorado.cires.wod.iquodqc.common.DoubleUtils.doubleEquals;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static java.lang.Math.max;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class WodGradientCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.WOD_GRADIENT_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    for (int i = 0; i < depths.size()-1; i++) {
      double depth1 = depths.get(i).getDepth();
      double depth2 = depths.get(i+1).getDepth();
      double depthDiff = depth2 - depth1;
      Optional<Double> temp1= getTemperature(depths.get(i)).map(v -> v.getValue());
      Optional<Double> temp2= getTemperature(depths.get(i+1)).map(v -> v.getValue());
      if (temp1.isPresent() && temp2.isPresent() &&
          !doubleEquals(Optional.of(depth1),Optional.of(depth2)) &&
          depthDiff > 0){
        double gradient =(temp2.get()- temp1.get()) / max(depthDiff, 3.0);
        if (gradient>0.3 || gradient <-0.7){
          failures.add(i);
          failures.add(i+1);
        }

/*        Test not needed - already caught
          # zero sensitivity indicator
          # flags data if temperature drops to 0 too abruptly, indicating a missing value.
          if t.data[i+1] == 0:
              if -1.0 * gradient > 5.0 * 0.7:  gradient < -3.5 < -0.7
                  qc[i+1] = True
 */
      }
    }
    return failures;
  }

}
