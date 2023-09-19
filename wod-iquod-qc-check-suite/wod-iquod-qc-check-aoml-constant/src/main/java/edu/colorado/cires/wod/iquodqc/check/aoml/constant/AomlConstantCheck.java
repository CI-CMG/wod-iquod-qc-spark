package edu.colorado.cires.wod.iquodqc.check.aoml.constant;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static edu.colorado.cires.wod.iquodqc.common.DoubleUtils.doubleEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AomlConstantCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.AOML_CONSTANT.getName();
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    int count = 0;
    int counti = 0;
    Optional<Double> temp1 = null;
    for (int i = 0; i < depths.size(); i++) {
      Optional<Double> temperature = getTemperature(depths.get(i)).map(v -> v.getValue());
      if (temperature.isPresent()) {
        if (count == 0) {
          temp1 = temperature;
        }
        if (!doubleEquals(temperature, temp1)) {
          return new LinkedHashSet<>();
        }
        count += 1;
      }
    }
    if (count == 1) {
      return new LinkedHashSet<>();
    }

    return IntStream.iterate(0, i -> i + 1)
        .limit(depths.size())
        .boxed()
        .collect(Collectors.toList());
  }

}
