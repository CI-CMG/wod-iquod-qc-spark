package edu.colorado.cires.wod.iquodqc.common.refdata.en;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Precision;
import software.amazon.awssdk.services.s3.S3Client;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public final class EnBgCheckInfoParametersReader {

  public final static String EN_BG_AVAIL_CHECK_NC_PROP = "EN_bgcheck_info.netcdf.uri";

  private final static String NAME = "EN_bgcheck_info";
  private final static int CONNECT_TIMEOUT = 2000;
  private final static int READ_TIMEOUT = 1000 * 60 * 15;

  private static EnBgCheckInfoParameters readEnBackgroundCheckAux(Path bgcheckInfoFile) throws IOException {
    try (NetcdfFile nc = NetcdfFiles.open(bgcheckInfoFile.toString())) {
      EnBgCheckInfoParameters parameters = new EnBgCheckInfoParameters();
      parameters.setLon(Objects.requireNonNull(nc.findVariable("longitude")).read());
      parameters.setLat(Objects.requireNonNull(nc.findVariable("latitude")).read());
      parameters.setDepth(Objects.requireNonNull(nc.findVariable("depth")).read());
      parameters.setMonth(Objects.requireNonNull(nc.findVariable("month")).read());
      parameters.setClim(Objects.requireNonNull(nc.findVariable("potm_climatology")).read());
      parameters.setFillValue(nc.findVariable("potm_climatology").findAttribute("_FillValue").getNumericValue().doubleValue());
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
    String uri = properties.getProperty(EN_BG_AVAIL_CHECK_NC_PROP);
    System.err.println("Downloading " + uri);
    Path ncFile;
    try {
      ncFile = Files.createTempFile(NAME + "_", ".nc");
    } catch (IOException e) {
      throw new RuntimeException("Unable to create temp file", e);
    }
    try {
      if (uri.startsWith("s3://")) {
        //TODO make this more robust, region, creds, etc
        S3Client s3 = S3Client.builder().build();
        String bucket = uri.replaceAll("s3://", "").split("/")[0];
        String key = uri.replaceAll("s3://", "").split("/", 2)[1];
        try (InputStream in = new BufferedInputStream(s3.getObject(c -> c.bucket(bucket).key(key)));
            OutputStream out = new BufferedOutputStream(Files.newOutputStream(ncFile))) {
          IOUtils.copy(in, out);
        }
      } else if (uri.startsWith("http://") || uri.startsWith("https://")){
        FileUtils.copyURLToFile(
            new URL(uri),
            ncFile.toFile(),
            CONNECT_TIMEOUT,
            READ_TIMEOUT);
      } else {
        FileUtils.copyFile(new File(uri), ncFile.toFile());
      }

      EnBgCheckInfoParameters parameters = readEnBackgroundCheckAux(ncFile);
      validateGrid(parameters);
      System.err.println("Done downloading " + uri);
      return parameters;
    } catch (IOException e) {
      throw new RuntimeException("Unable to download " + uri, e);
    } finally {
      FileUtils.deleteQuietly(ncFile.toFile());
    }

  }
}
