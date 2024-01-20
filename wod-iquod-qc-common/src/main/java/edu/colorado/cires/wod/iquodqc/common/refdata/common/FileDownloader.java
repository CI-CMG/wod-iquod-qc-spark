package edu.colorado.cires.wod.iquodqc.common.refdata.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.services.s3.S3Client;

public final class FileDownloader {

  public final static String DATA_DIR_PROP = "data.dir";
  private final static int CONNECT_TIMEOUT = 2000;
  private final static int READ_TIMEOUT = 1000 * 60 * 15;

  public static <T> T loadParameters(Properties properties, String propertyName, Function<Path, T> fileHandler) {
    return loadParameters(Paths.get(properties.getProperty(DATA_DIR_PROP)), properties, propertyName, fileHandler);
  }

  public static <T> T loadParameters(Path dataDir, Properties properties, String propertyName, Function<Path, T> fileHandler) {
    String uri = properties.getProperty(propertyName);
    Path ncFile = dataDir.resolve(propertyName + ".dat");
    synchronized (FileDownloader.class) {
      try {
        Files.createDirectories(dataDir);

      } catch (IOException e) {
        throw new RuntimeException("Unable to create temp file", e);
      }
      if (!Files.exists(ncFile)) {

        System.err.println("Downloading " + uri);
        try {
          Path tmp = Files.createTempFile(dataDir, propertyName, ".dat.dl");

          if (uri.startsWith("s3://")) {
            //TODO make this more robust, region, creds, etc
            S3Client s3 = S3Client.builder().build();
            String bucket = uri.replaceAll("s3://", "").split("/")[0];
            String key = uri.replaceAll("s3://", "").split("/", 2)[1];
            try (InputStream in = new BufferedInputStream(s3.getObject(c -> c.bucket(bucket).key(key)));
                OutputStream out = new BufferedOutputStream(Files.newOutputStream(tmp))) {
              IOUtils.copy(in, out);
            }
          } else if (uri.startsWith("http://") || uri.startsWith("https://") || uri.startsWith("ftp://") || uri.startsWith("ftps://")) {
            FileUtils.copyURLToFile(
                new URL(uri),
                tmp.toFile(),
                CONNECT_TIMEOUT,
                READ_TIMEOUT);
          } else {
            FileUtils.copyFile(new File(uri), tmp.toFile());
          }
          if (!Files.exists(ncFile)) {
            Files.move(tmp, ncFile, StandardCopyOption.REPLACE_EXISTING);
          } else {
            Files.delete(tmp);
          }
        } catch (IOException e) {
          throw new RuntimeException("Unable to download " + uri, e);
        }
        System.err.println("Done downloading " + uri);
      }
    }
    return fileHandler.apply(ncFile);
  }

  private FileDownloader() {

  }
}
