package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.util.Properties;

public class CarsParametersReader {

  public static final String CARS_NC_PROP = "cars.netcdf.uri";

  public static CarsParameters loadParameters(Properties properties) {
    return FileDownloader.loadParameters(properties, CARS_NC_PROP, CarsParameters::new);
  }

}
