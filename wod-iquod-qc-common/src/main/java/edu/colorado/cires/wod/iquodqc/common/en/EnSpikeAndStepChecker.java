package edu.colorado.cires.wod.iquodqc.common.en;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepFunctions.conditionA;
import static edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepFunctions.conditionB;
import static edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepFunctions.conditionC;
import static edu.colorado.cires.wod.iquodqc.common.en.EnSpikeAndStepFunctions.determineDepthTolerance;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.math3.util.Precision;

public final class EnSpikeAndStepChecker {

  private EnSpikeAndStepChecker() {

  }

  public static Collection<Integer> getFailedDepths(Cast cast, boolean suspect) {

    Set<Integer> failures = new LinkedHashSet<>();

    List<Depth> depths = cast.getDepths();

    EnSpikeAndStepInitializer initializer = new EnSpikeAndStepInitializer(cast);

    List<Double> t = initializer.getT();
    double[] z = initializer.getZ();
    List<Double> dt = initializer.getDt();
    List<Double> gt = initializer.getGt();

    for (int i = 1; i < depths.size(); i++) {
      Optional<ProfileData> maybeTi = getTemperature(depths.get(i));
      Optional<ProfileData> maybeTiMinus1 = getTemperature(depths.get(i - 1));
      if (maybeTi.isPresent() && maybeTiMinus1.isPresent()) {
        double ti = maybeTi.get().getValue();
        double tiMinus1 = maybeTiMinus1.get().getValue();
        double zi = depths.get(i).getDepth();
        double ziMinus1 = depths.get(i - 1).getDepth();
        Double wt1 = null;
        if (i == 1) {
          wt1 = 0.5;
        } else {
          Optional<ProfileData> maybeTiMinus2 = getTemperature(depths.get(i - 2));
          if (maybeTiMinus2.isPresent()) {
            double tiMinus2 = maybeTiMinus2.get().getValue();
            double ziMinus2 = depths.get(i - 2).getDepth();
            if (zi - ziMinus2 >= 5.0) {
              wt1 = (ziMinus1 - ziMinus2) / (zi - ziMinus2);
            } else {
              wt1 = 0.5;
            }
          }
        }
        if (wt1 != null) {
          double dTTol = determineDepthTolerance(ziMinus1, Math.abs(cast.getLatitude()));
          double gTTol = 0.05;
          // Check for low temperatures in the Tropics.
          // This might be more appropriate to appear in a separate EN regional
          // range check but is included here for now for consistency with the
          // original code.
          if (Math.abs(cast.getLatitude()) < 20.0 && ziMinus1 < 1000.0 && tiMinus1 < 1.0) {
            dt.set(i, null);
            if (suspect) {
              failures.add(i - 1);
            }
            continue;
          }
          conditionA(dt, dTTol, failures, wt1, i, suspect);
          conditionB(dt, dTTol, gTTol, failures, i, suspect, gt);
          conditionC(dt, dTTol, z, failures, t, i, suspect);
        }
      }
    }

    // Step or 0.0 at the bottom of a profile.
    if (suspect) {
      if (depths.size() > 0) {
        getTemperature(depths.get(depths.size() - 1)).map(ProfileData::getValue).ifPresent(temp -> {
          if (Precision.equals(temp, 0D)) {
            failures.add(depths.size() - 1);
          } else if (dt.get(depths.size() - 1) != null) {
            double dTTol = determineDepthTolerance(z[depths.size() - 1], Math.abs(cast.getLatitude()));
            if (Math.abs(dt.get(depths.size() - 1)) > dTTol) {
              failures.add(depths.size() - 1);
            }
          }
        });
      }
    } else if (failures.size() > depths.size() / 2) {
      // If 4 levels or more than half the profile is rejected then reject all.
      for (int i = 0; i < depths.size(); i++) {
        failures.add(i);
      }
    }

    return failures;
  }

}
