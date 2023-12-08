package edu.colorado.cires.wod.iquodqc.check.cotede.gradient;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getPressure;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getSalinity;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class CoTeDeGradientCheck extends CommonCastCheck {
  
  private static final double TEMPERATURE_THRESHOLD = 1.5;
  private static final double PRESSURE_THRESHOLD = 1.5;
  private static final double SALINITY_THRESHOLD = 1.5;

  @Override
  public String getName() {
    return "COTEDE_GRADIENT_CHECK";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    Set<Integer> failedDepths = new HashSet<>();
    
    failedDepths.addAll(
        CoTeDeGradient.checkGradient(
            cast.getDepths().stream()
                .map(d ->
                  getTemperature(d)
                      .map(ProfileData::getValue)
                      .orElse(Double.NaN)
                ).mapToDouble(Double::doubleValue)
                .toArray(),
            TEMPERATURE_THRESHOLD
        )
    );

    failedDepths.addAll(
        CoTeDeGradient.checkGradient(
            cast.getDepths().stream()
                .map(d ->
                    getPressure(d)
                        .map(ProfileData::getValue)
                        .orElse(Double.NaN)
                ).mapToDouble(Double::doubleValue)
                .toArray(),
            PRESSURE_THRESHOLD
        )
    );

    failedDepths.addAll(
        CoTeDeGradient.checkGradient(
            cast.getDepths().stream()
                .map(d ->
                    getSalinity(d)
                        .map(ProfileData::getValue)
                        .orElse(Double.NaN)
                ).mapToDouble(Double::doubleValue)
                .toArray(),
            SALINITY_THRESHOLD
        )
    );
    
    return failedDepths.stream()
        .sorted()
        .collect(Collectors.toList());
  }
}
