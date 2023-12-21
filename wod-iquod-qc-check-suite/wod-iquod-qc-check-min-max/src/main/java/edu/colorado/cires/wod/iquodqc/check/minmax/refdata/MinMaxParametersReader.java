package edu.colorado.cires.wod.iquodqc.check.minmax.refdata;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.util.Properties;

public class MinMaxParametersReader {
  
  public static final String WOD_TEMP_MIN_MAX_PROP = "wod_temp_min_max.netcdf.uri";
  public static final String WOD_INFO_DGG4H6_PROP = "wod_info_dgg4h6.mat.uri";
  
  public static void downloadTempMinMax(Properties properties) {
    FileDownloader.loadParameters(properties, WOD_TEMP_MIN_MAX_PROP, (path) -> null);
  }
  
  public static void downloadInfoDGG4H6(Properties properties) {
    FileDownloader.loadParameters(properties, WOD_INFO_DGG4H6_PROP, (path -> null));
  }

}
