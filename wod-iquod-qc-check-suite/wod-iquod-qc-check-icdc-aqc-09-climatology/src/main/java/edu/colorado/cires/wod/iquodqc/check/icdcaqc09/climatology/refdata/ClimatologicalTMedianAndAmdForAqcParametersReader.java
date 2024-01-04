package edu.colorado.cires.wod.iquodqc.check.icdcaqc09.climatology.refdata;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.util.Properties;

public class ClimatologicalTMedianAndAmdForAqcParametersReader {
  
  public static final String CLIMATOLOGICAL_T_MEDIAN_AND_AMD_FOR_AQC_NC_PROP = "climatological_t_median_and_amd_for_aqc.netcdf.uri";
  
  public static String loadParameters(Properties properties) {
    return FileDownloader.loadParameters(
        properties,
        CLIMATOLOGICAL_T_MEDIAN_AND_AMD_FOR_AQC_NC_PROP,
        (f) -> f.toFile().toString()
    );
  }

}
