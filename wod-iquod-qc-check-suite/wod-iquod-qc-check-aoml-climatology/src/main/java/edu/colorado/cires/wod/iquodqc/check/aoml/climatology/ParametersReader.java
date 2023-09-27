package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public final class ParametersReader {

  public final static String WOA_13_00_025_PROP = "woa13_00_025.netcdf.uri";
  // ftp://ftp.aoml.noaa.gov/phod/pub/bringas/XBT/AQC/AOML_AQC_2018/data_center/woa13_00_025.nc

  private static WoaDataHolder read(Path ncFile) throws IOException {
    try (NetcdfFile nc = NetcdfFiles.open(ncFile.toString())) {
      WoaDataHolder woaDataHolder = new WoaDataHolder();
      woaDataHolder.setTime(Objects.requireNonNull(nc.findVariable("time")).readScalarFloat());
      woaDataHolder.setDepths((float[]) Objects.requireNonNull(nc.findVariable("depth")).read().copyTo1DJavaArray());
      woaDataHolder.setLatitudes((float[]) Objects.requireNonNull(nc.findVariable("lat")).read().copyTo1DJavaArray());
      woaDataHolder.setLongitudes((float[]) Objects.requireNonNull(nc.findVariable("lon")).read().copyTo1DJavaArray());
      woaDataHolder.setNcFile(ncFile);
      return woaDataHolder;
    }
  }


  public static WoaDataHolder loadParameters(Properties properties) {
    return FileDownloader.loadParameters(properties, WOA_13_00_025_PROP, (ncFile) -> {
      try {
        return read(ncFile);
      } catch (IOException e) {
        throw new RuntimeException("Unable to process data file", e);
      }
    });
  }

  public static NetcdfFile open(Properties properties) {
    return FileDownloader.loadParameters(properties, WOA_13_00_025_PROP, (ncFile) -> {
      try {
        return NetcdfFiles.open(ncFile.toString());
      } catch (IOException e) {
        throw new RuntimeException("Unable to open file", e);
      }
    });
  }


}
