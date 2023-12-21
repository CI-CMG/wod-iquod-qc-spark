package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Function;

public class WoaParametersReader {

  public final static String WOA_S1_NC_PROP = "woa_s1.netcdf.uri";
  public final static String WOA_S2_NC_PROP = "woa_s2.netcdf.uri";
  public final static String WOA_S3_NC_PROP = "woa_s3.netcdf.uri";
  public final static String WOA_S4_NC_PROP = "woa_s4.netcdf.uri";

  public static WoaParameters loadParameters(Properties properties) {
    Path s1Path = FileDownloader.loadParameters(properties, WOA_S1_NC_PROP, Function.identity());
    Path s2Path = FileDownloader.loadParameters(properties, WOA_S2_NC_PROP, Function.identity());
    Path s3Path = FileDownloader.loadParameters(properties, WOA_S3_NC_PROP, Function.identity());
    Path s4Path = FileDownloader.loadParameters(properties, WOA_S4_NC_PROP, Function.identity());
    return new WoaParameters(s1Path, s2Path, s3Path, s4Path);
  }
}
