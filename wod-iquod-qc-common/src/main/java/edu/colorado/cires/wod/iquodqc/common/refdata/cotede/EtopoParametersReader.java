package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.util.Properties;

public class EtopoParametersReader {

  public final static String ETOPO5_NC_PROP = "etopo5.netcdf.uri";

  public static NetCdfEtopoParameters loadParameters(Properties properties) {
    return FileDownloader.loadParameters(properties, ETOPO5_NC_PROP, NetCdfEtopoParameters::new);
  }
}
