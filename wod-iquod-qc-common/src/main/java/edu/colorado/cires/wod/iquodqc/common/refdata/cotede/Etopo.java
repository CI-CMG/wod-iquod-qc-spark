package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public class Etopo {

  public final static String ETOPO5_NC_PROP = "etopo5.netcdf.uri";

  private static <T> T loadData(Properties properties, Function<NetcdfFile, T> doWithNc) {
    return FileDownloader.loadParameters(properties, ETOPO5_NC_PROP, (ncFile) -> {
      try (NetcdfFile nc = NetcdfFiles.open(ncFile.toString())) {
        return doWithNc.apply(nc);
      } catch (IOException e) {
        throw new RuntimeException("Unable to read NetCdf data", e);
      }
    });
  }

  //TODO
  private static float getBathymetry(NetcdfFile nc, double lon, double lat) {
    try {
      return -Objects.requireNonNull(nc.findVariable("ROSE")).read(new int[]{0, 0}, new int[]{1}).getFloat(0);
    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException("Unable to read NetCdf data", e);
    }

  }


}
