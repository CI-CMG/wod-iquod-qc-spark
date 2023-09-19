package edu.colorado.cires.wod.iquodqc.check.en.constant;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.en.constant.TemperatureCounter.TemperatureCount;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class EnConstantValueCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.EN_CONSTANT_VALUE_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Integer> failures = new LinkedList<>();

    double minDepth = cast.getDepths().stream().map(Depth::getDepth).min(Double::compareTo).orElse(0D);
    double maxDepth = cast.getDepths().stream().map(Depth::getDepth).max(Double::compareTo).orElse(0D);

    if (maxDepth - minDepth >= 100D) {
      TemperatureCounter temperatureCounter = new TemperatureCounter();
      cast.getDepths().stream()
          .map(DepthUtils::getTemperature)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .map(ProfileData::getValue)
          .mapToDouble(Double::doubleValue)
          .forEach(temperatureCounter::increment);
      long depthCount = cast.getDepths().stream().filter(depth -> DepthUtils.getTemperature(depth).isPresent()).count();
      int threshold = (int) Math.ceil(0.9 * (double) depthCount);
      int maxCount = temperatureCounter.getCounts().stream().mapToInt(TemperatureCount::getCount).max().orElse(0);
      if (maxCount >= threshold) {
        for (int i = 0; i < cast.getDepths().size(); i++) {
          failures.add(i);
        }
      }
    }

    return failures;
  }

}
