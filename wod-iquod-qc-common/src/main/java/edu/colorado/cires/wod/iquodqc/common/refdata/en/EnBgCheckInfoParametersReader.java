package edu.colorado.cires.wod.iquodqc.common.refdata.en;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.math3.util.Precision;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public final class EnBgCheckInfoParametersReader {

  public final static String EN_BG_AVAIL_CHECK_NC_PROP = "EN_bgcheck_info.netcdf.uri";

  private final static String NAME = "EN_bgcheck_info";


  private static EnBgCheckInfoParameters readEnBackgroundCheckAux(Path bgcheckInfoFile) throws IOException {
    try (NetcdfFile nc = NetcdfFiles.open(bgcheckInfoFile.toString())) {
      EnBgCheckInfoParameters parameters = new EnBgCheckInfoParameters();
      parameters.setLon(Objects.requireNonNull(nc.findVariable("longitude")).read());
      parameters.setLat(Objects.requireNonNull(nc.findVariable("latitude")).read());
      parameters.setDepth(Objects.requireNonNull(nc.findVariable("depth")).read());
      parameters.setMonth(Objects.requireNonNull(nc.findVariable("month")).read());
      parameters.setClim(Objects.requireNonNull(nc.findVariable("potm_climatology")).read());
      parameters.setClimFillValue(nc.findVariable("potm_climatology").findAttribute("_FillValue").getNumericValue().doubleValue());
      parameters.setBgevFillValue(nc.findVariable("bg_err_var").findAttribute("_FillValue").getNumericValue().doubleValue());
      parameters.setObevFillValue(nc.findVariable("ob_err_var").findAttribute("_FillValue").getNumericValue().doubleValue());
      parameters.setBgev(Objects.requireNonNull(nc.findVariable("bg_err_var")).read());
      parameters.setObev(Objects.requireNonNull(nc.findVariable("ob_err_var")).read());
      return parameters;
    }
  }

  private static void validateGrid(EnBgCheckInfoParameters parameters) {
    double lonSize = Math.abs(parameters.getLon().getDouble(1) - parameters.getLon().getDouble(0));
    double latSize = Math.abs(parameters.getLat().getDouble(1) - parameters.getLat().getDouble(0));
    for (int i = 1; i < parameters.getLon().getSize(); i++) {
      double size = Math.abs(parameters.getLon().getDouble(i) - parameters.getLon().getDouble(i - 1));
      if (!Precision.equals(lonSize, size)) {
        throw new IllegalArgumentException("Invalid Background Check File: Longitude not evenly spaced");
      }
    }
    for (int i = 1; i < parameters.getLat().getSize(); i++) {
      double size = Math.abs(parameters.getLat().getDouble(i) - parameters.getLat().getDouble(i - 1));
      if (!Precision.equals(latSize, size)) {
        throw new IllegalArgumentException("Invalid Background Check File: Latitude not evenly spaced");
      }
    }
    parameters.setLonGridSize(lonSize);
    parameters.setLatGridSize(latSize);
  }

  public static EnBgCheckInfoParameters loadParameters(Properties properties) {
    return FileDownloader.loadParameters(properties, EN_BG_AVAIL_CHECK_NC_PROP, (ncFile) -> {
      try {
        EnBgCheckInfoParameters parameters = readEnBackgroundCheckAux(ncFile);
        validateGrid(parameters);
        return parameters;
      } catch (IOException e) {
        throw new RuntimeException("Unable to process background file", e);
      }
    });
  }
}
