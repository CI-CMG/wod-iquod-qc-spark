package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.iquodqc.common.InterpolationUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.spark.sql.Row;
import org.geotools.referencing.GeodeticCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;

public class AomlClimatologyCheck extends CommonCastCheck {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(AomlClimatologyCheck.class);

  private static WoaDataHolder parameters;
  private Properties properties;

  @Override
  public String getName() {
    return CheckNames.AOML_CLIMATOLOGY.getName();
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
    Set<Integer> failed = new TreeSet<>();
    NetcdfFile ncFile = ParametersReader.open(properties);
    try {
      List<Depth> depths = cast.getDepths();
      GeodeticCalculator geodeticCalculator = InterpolationUtils.createCalculator();
      List<TempAtPosition> tAnTemps = AomlClimatologyUtils.getPositionTempsAtAllDepths(ncFile, "t_an", parameters, cast.getLongitude(), cast.getLatitude(), geodeticCalculator);
      List<TempAtPosition> tSdTemps = AomlClimatologyUtils.getPositionTempsAtAllDepths(ncFile, "t_sd", parameters, cast.getLongitude(), cast.getLatitude(), geodeticCalculator);
      for (int depthIndex = 0; depthIndex < cast.getDepths().size(); depthIndex++) {
        int i = depthIndex;
        Depth depth = cast.getDepths().get(depthIndex);
        DepthUtils.getTemperature(depth).ifPresent(tpd -> {
          double temperature = tpd.getValue();
          PolynomialSplineFunction tAnInterpolator = AomlClimatologyUtils.fitInterpolator(
              tAnTemps,
              cast.getLongitude(),
              cast.getLatitude(),
              depths.get(i).getDepth(),
              false
          );
          OptionalDouble analyzedMean;
          if (tAnInterpolator == null) {
            analyzedMean = OptionalDouble.empty();
          } else {
            analyzedMean = InterpolationUtils.interpolate(depth.getDepth(), tAnInterpolator);
          }
          analyzedMean.ifPresent(interpTemp -> {
            PolynomialSplineFunction tSdInterpolator = AomlClimatologyUtils.fitInterpolator(
                tSdTemps,
                cast.getLongitude(),
                cast.getLatitude(),
                depths.get(i).getDepth(),
                true
            );
            OptionalDouble standardDeviation;
            if (tSdInterpolator == null) {
              standardDeviation = OptionalDouble.empty();
            } else {
              standardDeviation = InterpolationUtils.interpolate(depth.getDepth(), tSdInterpolator);
            }
            standardDeviation.ifPresent(interpTempSd -> {
              if (interpTempSd > 0d && Math.abs(temperature - interpTemp ) / interpTempSd > 5d) {
                failed.add(i);
              }
            });
          });
        });
      }
    } finally {
      try {
        ncFile.close();
      } catch (IOException e) {
        LOGGER.error("{}: Unable to close NetCDF file: "+ e.getMessage(), getName());
      }
    }

    return failed;
  }


  private static void loadParameters(Properties properties) {
    synchronized (AomlClimatologyCheck.class) {
      if (parameters == null) {
        parameters = ParametersReader.loadParameters(properties);
      }
    }
  }

}
