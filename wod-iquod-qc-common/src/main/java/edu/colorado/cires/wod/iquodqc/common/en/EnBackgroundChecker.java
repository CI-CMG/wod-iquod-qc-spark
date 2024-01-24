package edu.colorado.cires.wod.iquodqc.common.en;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;
import static edu.colorado.cires.wod.iquodqc.common.en.PgeEstimator.estimateProbabilityOfGrossError;

import edu.colorado.cires.mgg.teosgsw.TeosGsw10;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.common.CastUtils;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.iquodqc.common.ObsUtils;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.CastParameterDataReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.ParameterDataReader;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import javax.annotation.Nullable;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

public class EnBackgroundChecker {

  private static final double DEFAULT_SALINITY = 35D;

  private final EnBgCheckInfoParameters parameters;
  private final ParameterDataReader parameterData;
  private final List<Double> obevArray;
  private final List<Double> depthArray;

  public EnBackgroundChecker(EnBgCheckInfoParameters parameters) {
    this.parameters = parameters;
    parameterData = new ParameterDataReader(parameters);
    obevArray = parameterData.getObev();
    depthArray = parameterData.getDepths();
  }

  private static class ArrayWrapper {

    double[] depths;
    double[] clim;
    double[] bgev;
    double[] obev;
  }

  private ArrayWrapper getParameterData(Cast cast) {
    CastParameterDataReader castParameterData = new CastParameterDataReader(cast, parameters);
    List<Double> climArray = castParameterData.getClim();
    List<Double> bgevArray = castParameterData.getBgev();

    List<Double> climArrayOk = new ArrayList<>(depthArray.size());
    List<Double> bgevArrayOk = new ArrayList<>(depthArray.size());
    List<Double> obevArrayOk = new ArrayList<>(depthArray.size());
    List<Double> depthArrayOk = new ArrayList<>(depthArray.size());

    for (int i = 0; i < depthArray.size(); i++) {
      if (climArray.get(i) != null && bgevArray.get(i) != null && obevArray.get(i) != null && depthArray.get(i) != null) {
        climArrayOk.add(climArray.get(i));
        bgevArrayOk.add(bgevArray.get(i));
        obevArrayOk.add(obevArray.get(i));
        depthArrayOk.add(depthArray.get(i));
      }
    }

    ArrayWrapper wrapper = new ArrayWrapper();
    wrapper.depths = depthArrayOk.stream().mapToDouble(Double::doubleValue).toArray();
    wrapper.clim = climArrayOk.stream().mapToDouble(Double::doubleValue).toArray();
    wrapper.bgev = bgevArrayOk.stream().mapToDouble(Double::doubleValue).toArray();
    wrapper.obev = obevArrayOk.stream().mapToDouble(Double::doubleValue).toArray();

    return wrapper;
  }

  public EnBackgroundCheckerResult getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    EnBackgroundCheckerResult enBackgroundCheckerResult = new EnBackgroundCheckerResult();

    List<Depth> levels = cast.getDepths();

    ArrayWrapper parameterData = getParameterData(cast);

    if (parameterData.depths.length == 1) {
      return enBackgroundCheckerResult;
    }

    double[] pressures = levels.stream()
        .mapToDouble(Depth::getDepth)
        .map(depth -> TeosGsw10.INSTANCE.gsw_p_from_z(-depth, cast.getLatitude(), 0D, 0D))
        .toArray();

    PolynomialSplineFunction climFunction = new LinearInterpolator().interpolate(parameterData.depths, parameterData.clim);
    PolynomialSplineFunction bgevFunction = new LinearInterpolator().interpolate(parameterData.depths, parameterData.bgev);
    PolynomialSplineFunction obevFunction = new LinearInterpolator().interpolate(parameterData.depths, parameterData.obev);

    Integer probeType = CastUtils.getProbeType(cast).map(Attribute::getValue).map(Double::intValue).orElse(null);

    for (int i = 0; i < levels.size(); i++) {
      final int iLevel = i;
      Depth level = levels.get(iLevel);
      getTemperature(level).ifPresent(tempProfile -> {
        //  Get the climatology and error variance values at this level.
        interpolate(level.getDepth(), climFunction).ifPresent(climLevel -> {
          interpolate(level.getDepth(), bgevFunction).ifPresent(bgevLevel -> {
            if (bgevLevel <= 0D) {
              throw new IllegalArgumentException("Background error variance <= 0");
            }
            interpolate(level.getDepth(), obevFunction).ifPresent(obevLevel -> {
              if (obevLevel <= 0D) {
                throw new IllegalArgumentException("Observation error variance <= 0");
              }
              boolean failedSpikeTest = otherTestResults.get(CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName()).getFailedDepths().contains(iLevel);
              double pressure = pressures[iLevel];
              isCheckFailed(cast, level, tempProfile.getValue(), climLevel, bgevLevel, obevLevel, probeType, pressure, failedSpikeTest,
                  enBackgroundCheckerResult, iLevel);
            });
          });
        });
      });
    }

    return enBackgroundCheckerResult;
  }

  private static void isCheckFailed(
      Cast cast,
      Depth depth,
      double temp,
      double climLevel,
      double bgevLevel,
      double obevLevel,
      @Nullable Integer probeType,
      double pressure,
      boolean failedSpikeTest,
      EnBackgroundCheckerResult checkerResult,
      int i
  ) {
    bgevLevel = correctBgLevelForLatitude(bgevLevel, cast);
    double pge = estimateProbabilityOfGrossError(probeType, failedSpikeTest);
    double sLevel = DepthUtils.getSalinity(depth).map(ProfileData::getValue).orElse(DEFAULT_SALINITY);
    double potm = ObsUtils.pottem(temp, sLevel, pressure, 0D, true, 0D);
    double evLevel = obevLevel + bgevLevel;
    double sdiff = Math.pow(potm - climLevel, 2D) / evLevel;
    double pdGood = Math.exp(-0.5 * Math.min(sdiff, 160D)) / Math.sqrt(2D * Math.PI * evLevel);
    double pdTotal = 0.1 * pge + pdGood * (1D - pge);
    double pgebk = 0.1 * pge / pdTotal;
    if (pgebk >= 0.5) {
      checkerResult.getFailures().add(i);
    } else {
      checkerResult.getLevels().add(new EnBackgroundCheckerLevelResult(i, potm, climLevel));
    }
  }

  private static double correctBgLevelForLatitude(double bgevLevel, Cast cast) {
    // If at low latitudes the background error variance is increased.
    // Also, because we are on reported levels instead of standard levels
    // the variances are increased. NB multiplication factors are squared
    // because we are working with error variances instead of standard
    // deviations.
    if (Math.abs(cast.getLatitude()) < 10D) {
      bgevLevel = bgevLevel * Math.pow(1.5, 2D);
    }
    return bgevLevel * Math.pow(2.0, 2D);
    // Original Python code
    //    if np.abs(p.latitude()) < 10.0: bgevLevel *= 1.5**2
    //    bgevLevel *= 2.0**2
  }


  private static OptionalDouble interpolate(double z, PolynomialSplineFunction f) {
    if (f.isValidPoint(z)) {
      // check this first to prevent an unnecessary array copy
      return OptionalDouble.of(f.value(z));
    } else {
      // same behavior as np.interp
      double[] knots = f.getKnots();
      if (z <= knots[0]) {
        return OptionalDouble.of(f.value(knots[0]));
      }
    }
    return OptionalDouble.empty();
  }

}
