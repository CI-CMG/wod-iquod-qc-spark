package edu.colorado.cires.wod.iquodqc.check.wod.looselocationatsea;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.EtopoParametersReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.NetCdfEtopoParameters;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.Row;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public class WodLooseLocationAtSeaCheck extends CommonCastCheck {
  
  private static final int BUFFER_WIDTH = 2;
  
  private static NetCdfEtopoParameters parameters;
  
  private Properties properties;

  @Override
  public String getName() {
    return "WOD_LOOSE_LOCATION_AT_SEA_CHECK";
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
    try (NetcdfFile netcdfFile = NetcdfFiles.open(properties.getProperty("data.dir") + File.separator + "etopo5.netcdf.uri.dat")) {
      if (WodLooseLocationAtSea.checkLooseLocationAtSea(cast.getLatitude(), cast.getLongitude(), BUFFER_WIDTH, netcdfFile)) {
        return Collections.emptyList();
      }
      return IntStream.range(0, cast.getDepths().size())
          .boxed()
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static void loadParameters(Properties properties) {
    synchronized (WodLooseLocationAtSeaCheck.class) {
      if (parameters == null) {
        parameters = EtopoParametersReader.loadParameters(properties);
      }
    }
  }
}
