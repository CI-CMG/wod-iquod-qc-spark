package edu.colorado.cires.wod.iquodqc.check.en.stability;

import static edu.colorado.cires.wod.iquodqc.check.en.stability.EnStabilityCheckFunctions.mcdougallEOS;
import static edu.colorado.cires.wod.iquodqc.check.en.stability.EnStabilityCheckFunctions.potentialTemperature;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getPressure;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getSalinity;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class EnStabilityCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return CheckNames.EN_STABILITY_CHECK.getName();
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new TreeSet<>();
    List<Double> potTemps = calculatePotentialTemperatures(cast);
    for (int index = 2; index < depths.size() - 1; index++) {
      int i = index;
      getTemperature(depths.get(i)).map(ProfileData::getValue).ifPresent(t -> {
        getTemperature(depths.get(i - 1)).map(ProfileData::getValue).ifPresent(t1 -> {
          getTemperature(depths.get(i - 2)).map(ProfileData::getValue).ifPresent(t2 -> {
            getPressure(depths.get(i)).map(ProfileData::getValue).ifPresent(p -> {
              getPressure(depths.get(i - 1)).map(ProfileData::getValue).ifPresent(p1 -> {
                getPressure(depths.get(i - 2)).map(ProfileData::getValue).ifPresent(p2 -> {
                  getSalinity(depths.get(i)).map(ProfileData::getValue).ifPresent(s -> {
                    getSalinity(depths.get(i - 1)).map(ProfileData::getValue).ifPresent(s1 -> {
                      getSalinity(depths.get(i - 2)).map(ProfileData::getValue).ifPresent(s2 -> {
                        Optional.ofNullable(potTemps.get(i)).ifPresent(potT -> {
                          Optional.ofNullable(potTemps.get(i - 1)).ifPresent(potT1 -> {
                            Optional.ofNullable(potTemps.get(i - 2)).ifPresent(potT2 -> {
                              calculateFailedDepths(
                                  failures, depths, potTemps, i,
                                  p, p1, p2,
                                  s, s1, s2,
                                  potT, potT1, potT2);
                            });
                          });
                        });
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    }

    if (depths.size() > 1) {
      // check bottom of profile
      final int i = depths.size() - 1;
      getTemperature(depths.get(i)).map(ProfileData::getValue).ifPresent(t -> {
        getTemperature(depths.get(i - 1)).map(ProfileData::getValue).ifPresent(t1 -> {
          getPressure(depths.get(i)).map(ProfileData::getValue).ifPresent(p -> {
            getPressure(depths.get(i - 1)).map(ProfileData::getValue).ifPresent(p1 -> {
              getSalinity(depths.get(i)).map(ProfileData::getValue).ifPresent(s -> {
                getSalinity(depths.get(i - 1)).map(ProfileData::getValue).ifPresent(s1 -> {
                  Optional.ofNullable(potTemps.get(i)).ifPresent(potT -> {
                    Optional.ofNullable(potTemps.get(i - 1)).ifPresent(potT1 -> {
                      double deltaRhoK = mcdougallEOS(s, potT, p) - mcdougallEOS(s1, potT1, p1);
                      if (deltaRhoK < -0.03) {
                        failures.add(i);
                      }
                    });
                  });
                });
              });
            });
          });
        });
      });
    }

    int failureCount = failures.size();
    int tempCount = (int) depths.stream().filter(depth -> getTemperature(depth).isPresent()).count();
    double threshold = Math.max(2D, (double) tempCount / 4D);

    // check for critical number of flags, flag all if so:
    if ((double)failureCount >= threshold) {
      for (int j = 0; j < depths.size(); j++) {
        failures.add(j);
      }
    }

    return failures;
  }

  private static List<Double> calculatePotentialTemperatures(Cast cast) {
    List<Double> potTemps = new ArrayList<>(cast.getDepths().size());
    cast.getDepths().forEach(depth -> {
      Double t = getTemperature(depth).map(ProfileData::getValue).orElse(null);
      Double p = getPressure(depth).map(ProfileData::getValue).orElse(null);
      Double s = getSalinity(depth).map(ProfileData::getValue).orElse(null);
      if (t == null || p == null || s == null) {
        potTemps.add(null);
      } else {
        potTemps.add(potentialTemperature(s, t, p));
      }
    });
    return potTemps;
  }

  private static void calculateFailedDepths(
      Set<Integer> failures,
      List<Depth> depths,
      List<Double> potTemps,
      int i,
      double p, double p1, double p2,
      double s, double s1, double s2,
      double potT, double potT1, double potT2
  ) {
    double deltaRhoK = mcdougallEOS(s, potT, p) - mcdougallEOS(s1, potT1, p);
    if (deltaRhoK < -0.03) {
      double deltaRhoKPrev = mcdougallEOS(s1, potT1, p1) - mcdougallEOS(s2, potT2, p1);
      if (Math.abs(deltaRhoKPrev + deltaRhoK) < 0.25 * Math.abs(deltaRhoKPrev - deltaRhoK)) {
        failures.add(i - 1);
      } else {
        getTemperature(depths.get(i + 1)).map(ProfileData::getValue).ifPresent(tNext -> {
          getPressure(depths.get(i + 1)).map(ProfileData::getValue).ifPresent(pNext -> {
            getSalinity(depths.get(i + 1)).map(ProfileData::getValue).ifPresent(sNext -> {
              Optional.ofNullable(potTemps.get(i + 1)).ifPresent(potTNext -> {
                double deltaRhoKNext = mcdougallEOS(sNext, potTNext, pNext) - mcdougallEOS(s, potT, pNext);
                if (Math.abs(deltaRhoK + deltaRhoKNext) < 0.25 * Math.abs(deltaRhoK - deltaRhoKNext)) {
                  failures.add(i);
                } else {
                  failures.add(i - 1);
                  failures.add(i);
                }
              });
            });
          });
        });
      }
    }
  }


}
