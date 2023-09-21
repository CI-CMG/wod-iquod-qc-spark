package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import edu.colorado.cires.wod.iquodqc.common.en.PgeEstimator;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.List;
import javax.annotation.Nullable;

final class BuddyCheckFunctions {


  private static final double corScaleA = 100D; // In km.
  private static final double corScaleB = 400D; // In km.
  private static final double corScaleT = 432000D; // 5 days in secs.

  static double buddyCovariance(double minDist, Cast cast, Cast buddyCast, double mesoEvA, double mesoEvB, double synEvA, double synEvB) {
    final double mesSDist = minDist / (1000D * corScaleA);
    final double synSDist = minDist / (1000D * corScaleB);
    final double timeDiff = Math.pow(Math.abs((cast.getTimestamp() - buddyCast.getTimestamp()) / 1000D) / corScaleT, 2D);
    return Math.sqrt(mesoEvA * mesoEvB) * (1D + mesSDist) * Math.exp(-mesSDist - timeDiff) + Math.sqrt(synEvA * synEvB) * (1D + synSDist) * Math.exp(
        -synSDist - timeDiff);
  }

  static double determinePge(int iLevel, double level, List<Double> bgev, List<Double> obev, double latitude, @Nullable Integer probeType) {
    double bgevLevel = bgev.get(iLevel);
    if (Math.abs(latitude) < 10D) {
      bgevLevel *= Math.pow(1.5, 2D);
    }
    double obevLevel = obev.get(iLevel);
    double pgeEst = PgeEstimator.estimateProbabilityOfGrossError(probeType, false);
    double kappa = 0.1;
    double evLevel = obevLevel + bgevLevel; // V from the text
    double sdiff = Math.pow(level, 2D) / evLevel;
    double pdGood = Math.exp(-0.5 * Math.min(sdiff, 160D)) / Math.sqrt(2D * Math.PI * evLevel);
    double pdTotal = kappa * pgeEst + pdGood * (1D - pgeEst);
    return kappa * pgeEst / pdTotal;
  }

  private BuddyCheckFunctions() {

  }
}
