package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata;

import edu.colorado.cires.wod.iquodqc.common.refdata.common.FileDownloader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

public class CarsParametersReader {
  
  public static final String CARS_NC_PROP = "cars.netcdf.uri";
  
  public static CarsParameters loadParameters(Properties properties) {
    return FileDownloader.loadParameters(properties, CARS_NC_PROP, (fp) -> {
      if (fp.toFile().exists()) {
        return new CarsParameters(fp);
      }
      String compressedFilePath = fp.toFile() + ".gz";
      boolean fileNameChanged = fp.toFile().renameTo(new File(compressedFilePath));
      if (!fileNameChanged) {
        throw new IllegalStateException("Failed to add .gz extension to " + fp);
      }
      try (
          InputStream inputStream = Files.newInputStream(Paths.get(compressedFilePath));
          GzipCompressorInputStream compressorInputStream = new GzipCompressorInputStream(inputStream);
          OutputStream outputStream = new FileOutputStream(fp.toFile())
      ) {
        IOUtils.copy(compressorInputStream, outputStream);
        return new CarsParameters(fp);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        FileUtils.deleteQuietly(new File(compressedFilePath));
      }
    });
  }

}
