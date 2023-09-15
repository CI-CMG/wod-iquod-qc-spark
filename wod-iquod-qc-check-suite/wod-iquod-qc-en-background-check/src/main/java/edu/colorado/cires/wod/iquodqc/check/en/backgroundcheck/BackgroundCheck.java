package edu.colorado.cires.wod.iquodqc.check.en.backgroundcheck;

import static edu.colorado.cires.wod.iquodqc.check.en.backgroundcheck.PgeEstimator.estimateProbabilityOfGrossError;
import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.mgg.teosgsw.TeosGsw10;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CastUtils;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.iquodqc.common.ObsUtils;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParametersReader;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.spark.sql.Row;

public class BackgroundCheck extends CommonCastCheck {

  private final static String NAME = "EN_background_check";
  private final static String EN_SPIKE_AND_STEP_SUSPECT = "EN_spike_and_step_suspect";
  private static final double DEFAULT_SALINITY = 35D;

  private static EnBgCheckInfoParameters parameters;
  private Properties properties;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> dependsOn() {
    return Collections.singleton(EN_SPIKE_AND_STEP_SUSPECT);
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (parameters == null) {
      loadParameters(properties);
    }
    return super.checkUdf(row);
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    throw new UnsupportedOperationException("This method is not used");
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    List<Depth> levels = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();

    ParameterDataReader parameterData = new ParameterDataReader(cast, parameters);
    double[] clim = parameterData.getClim();
    double[] bgev = parameterData.getBgev();
    double[] obev = parameterData.getObev();
    double[] depths = parameterData.getDepths();

    double[] pressures = levels.stream()
        .mapToDouble(Depth::getDepth)
        .map(depth -> TeosGsw10.INSTANCE.gsw_p_from_z(-depth, cast.getLatitude(), 0D, 0D))
        .toArray();

    PolynomialSplineFunction climFunction = new LinearInterpolator().interpolate(depths, clim);
    PolynomialSplineFunction bgevFunction = new LinearInterpolator().interpolate(depths, bgev);
    PolynomialSplineFunction obevFunction = new LinearInterpolator().interpolate(depths, obev);

    CastUtils.getProbeType(cast).map(Attribute::getValue).map(Double::intValue).ifPresent(probeType -> {
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
                boolean failedSpikeTest = otherTestResults.get(EN_SPIKE_AND_STEP_SUSPECT).getFailedDepths().contains(iLevel);
                double pressure = pressures[iLevel];
                if (isCheckFailed(cast, level, tempProfile.getValue(), climLevel, bgevLevel, obevLevel, probeType, pressure, failedSpikeTest)) {
                  failures.add(iLevel);
                }
              });
            });
          });
        });
      }
    });

    return failures;
  }

  private boolean isCheckFailed(
      Cast cast,
      Depth depth,
      double temp,
      double climLevel,
      double bgevLevel,
      double obevLevel,
      int probeType,
      double pressure,
      boolean failedSpikeTest
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
    return pgebk >= 0.5;
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


  private static void loadParameters(Properties properties) {
    synchronized (BackgroundCheck.class) {
      if (parameters == null) {
        parameters = EnBgCheckInfoParametersReader.loadParameters(properties);
      }
    }
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
