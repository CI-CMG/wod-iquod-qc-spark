package edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getPressure;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getSalinity;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CoTeDeTukey53HCheck extends CommonCastCheck {
  
  private static final double TEMPERATURE_THRESHOLD = 2.5;
  private static final double PRESSURE_THRESHOLD = 2.5;
  private static final double SALINITY_THRESHOLD = 2.5;

  @Override
  public String getName() {
    return "COTEDE_TUKEY_53_CHECK";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    Set<Integer> failedDepths = new HashSet<>();
    failedDepths.addAll(
        checkTukey53H(
            cast.getDepths().stream()
                .map(d ->
                    getTemperature(d)
                        .map(ProfileData::getValue)
                        .orElse(Double.NaN)
                ).mapToDouble(Double::doubleValue)
                .toArray(),
            getTemperatureThreshold()
        )
    );
    
    failedDepths.addAll(
        checkTukey53H(
            cast.getDepths().stream()
                .map(d ->
                    getPressure(d)
                        .map(ProfileData::getValue)
                        .orElse(Double.NaN)
                ).mapToDouble(Double::doubleValue)
                .toArray(),
              getPressureThreshold()
        )  
    );

    failedDepths.addAll(
        checkTukey53H(
            cast.getDepths().stream()
                .map(d ->
                    getSalinity(d)
                        .map(ProfileData::getValue)
                        .orElse(Double.NaN)
                ).mapToDouble(Double::doubleValue)
                .toArray(),
            getSalinityThreshold()
        )
    );
    
    return failedDepths.stream().sorted().collect(Collectors.toList());
  }
  
  protected double getPressureThreshold() {
    return PRESSURE_THRESHOLD;
  }
  
  protected double getTemperatureThreshold() {
    return TEMPERATURE_THRESHOLD;
  }
  
  protected double getSalinityThreshold() {
    return SALINITY_THRESHOLD;
  }
  
  protected List<Integer> checkTukey53H(double[] input, double threshold) {
    return CoTeDeTukey53H.checkTukey53H(
        input,
        threshold,
        false
    );
  }
}