package edu.colorado.cires.wod.iquodqc.check.en.range;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static java.lang.Math.abs;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class EnRangeCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.EN_RANGE_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();
    for (int i = 0; i < depths.size(); i++) {
      Optional<Double> temperature = getTemperature(depths.get(i)).map(v -> v.getValue());
      if (temperature.isPresent()) {
        double t = temperature.get();
        if((t < -4.0) | (t > 40.0)){
          failures.add(i);
        }
      }
    }
    return failures;
  }

}
