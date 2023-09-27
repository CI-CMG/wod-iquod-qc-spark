package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.io.IOException;
import java.util.Collection;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.spark.sql.Row;
import ucar.nc2.NetcdfFile;

public class AomlClimatologyCheck extends CommonCastCheck {

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
      for (int depthIndex = 0; depthIndex < cast.getDepths().size(); depthIndex++) {
        int i = depthIndex;
        Depth depth = cast.getDepths().get(depthIndex);
        DepthUtils.getTemperature(depth).ifPresent(tpd -> {
          double temperature = tpd.getValue();
          OptionalDouble analyzedMean = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_an", parameters, cast.getLongitude(), cast.getLatitude(), depth.getDepth(), false);
          analyzedMean.ifPresent(interpTemp -> {
            OptionalDouble standardDeviation = AomlClimatologyUtils.temperatureInterpolationProcess(ncFile, "t_sd", parameters, cast.getLongitude(), cast.getLatitude(), depth.getDepth(), true);
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
        System.err.println("Unable to close NetCDF file: "+ e.getMessage());
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
