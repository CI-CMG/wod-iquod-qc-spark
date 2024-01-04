package edu.colorado.cires.wod.iquodqc.check.icdcaqc10.climatology.refdata;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.util.Properties;

public class GlobalMedianQuartilesMedcoupleSmoothedParametersReader {
  
  public static final String GLOBAL_MEAN_MEDIAN_QUARTILES_MEDCOUPLE_SMOOTHED_NC_PROP = "global_mean_median_quartiles_medcouple_smoothed.netcdf.uri";

  public static String loadParameters(Properties properties) {
    return FileDownloader.loadParameters(
        properties,
        GLOBAL_MEAN_MEDIAN_QUARTILES_MEDCOUPLE_SMOOTHED_NC_PROP,
        (f) -> f.toFile().toString()
    );
  }

}
