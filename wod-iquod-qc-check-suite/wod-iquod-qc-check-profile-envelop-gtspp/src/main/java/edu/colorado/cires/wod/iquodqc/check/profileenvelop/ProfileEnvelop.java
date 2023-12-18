package edu.colorado.cires.wod.iquodqc.check.profileenvelop;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getPressure;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProfileEnvelop {
  
  public static List<Integer> checkProfileEnvelop(List<Depth> depths, List<ProfileThresholds> profiles) {
    return IntStream.range(0, depths.size()).boxed()
        .filter(i -> {
          Depth depth = depths.get(i);
          double pressure = getPressure(depth).map(ProfileData::getValue)
              .orElse(Double.NaN);
          if (Double.isNaN(pressure)) {
            return true;
          }
          double temperature = getTemperature(depth).map(ProfileData::getValue)
              .orElse(Double.NaN);
          if (Double.isNaN(temperature)) {
            return true;
          }

          Optional<ProfileThresholds> maybeProfile = profiles.stream()
              .filter(p ->
                  pressure > p.pressureThreshold.greaterThan && pressure <= p.pressureThreshold.lessThanOrEqualTo
              ).findFirst();

          if (maybeProfile.isEmpty()) {
            return false; // Could be configuration error, cannot rule out solely because there is no matching pressure threshold
          }

          ProfileThresholds profile = maybeProfile.get();
          return !(temperature > profile.temperatureThreshold.greaterThan && temperature < profile.temperatureThreshold.lessThan);
        })
        .collect(Collectors.toList());
  }

  public static class ProfileThresholds {
    private final PressureThreshold pressureThreshold;
    private final TemperatureThreshold temperatureThreshold;

    ProfileThresholds(double pressureGreaterThan, double pressureLessThanOrEqualTo, double temperatureGreaterThan, double temperatureLessThan) {
      this.pressureThreshold = new PressureThreshold(pressureGreaterThan, pressureLessThanOrEqualTo);
      this.temperatureThreshold = new TemperatureThreshold(temperatureGreaterThan, temperatureLessThan);
    }
  }

  public static class PressureThreshold {
    private final double greaterThan;
    private final double lessThanOrEqualTo;

    PressureThreshold(double greaterThan, double lessThanOrEqualTo) {
      this.greaterThan = greaterThan;
      this.lessThanOrEqualTo = lessThanOrEqualTo;
    }
  }

  public static class TemperatureThreshold {
    private final double greaterThan;
    private final double lessThan;

    TemperatureThreshold(double greaterThan, double lessThan) {
      this.greaterThan = greaterThan;
      this.lessThan = lessThan;
    }
  }

}
