package edu.colorado.cires.wod.iquodqc.check.cotede.digitrollover;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class CoTeDeDigitRolloverCheck extends CommonCastCheck {
  private static final double TEMPERATURE_THRESHOLD = 2.5;
  private static final double PRESSURE_THRESHOLD = 2.5;
  private static final double SALINITY_THRESHOLD = 2.5;

  @FunctionalInterface
  interface GetVariableValue {
    Optional<ProfileData> apply(Depth depth);
  }

  @Override
  public String getName() {
    return "COTEDE_DIGIT_ROLLOVER";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    return IntStream.range(1, depths.size())
        .filter(i -> !checkDigitRolloverAllFields(depths.get(i), depths.get(i - 1)))
        .boxed()
        .collect(Collectors.toList());
  }
  
  private static boolean checkDigitRolloverAllFields(Depth current, Depth previous) {
    return checkDigitRolloverField(current, previous, TEMPERATURE_THRESHOLD, DepthUtils::getTemperature) &&
        checkDigitRolloverField(current, previous, PRESSURE_THRESHOLD, DepthUtils::getPressure) &&
        checkDigitRolloverField(current, previous, SALINITY_THRESHOLD, DepthUtils::getSalinity);
  }
  
  private static boolean checkDigitRolloverField(Depth current, Depth previous, double threshold, GetVariableValue method) {
    return CoTeDeDigitRollover.checkDigitRollover(
        getValue(current, method),
        getValue(previous, method),
        threshold
    );
  }
  
  private static double getValue(Depth depth, GetVariableValue method) {
    return method.apply(depth)
        .map(ProfileData::getValue)
        .orElse(Double.NaN);
  }
}
