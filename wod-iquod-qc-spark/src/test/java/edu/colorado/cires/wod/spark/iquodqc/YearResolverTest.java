package edu.colorado.cires.wod.spark.iquodqc;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

public class YearResolverTest {

  @Test
  public void testFile() throws Exception {
    Path dir = Paths.get("target/wod-test-parquet");
    String path = "wod-parquet/yearly";
    String dataset = "CTD";
    String processingLevel = "OBS";
    FileUtils.deleteQuietly(dir.toFile());
    Path parquetDir = dir.resolve(path).resolve(dataset).resolve(processingLevel).resolve("CTDO2022.parquet");
    Files.createDirectories(parquetDir);
    Files.createFile(parquetDir.resolve("_SUCCESS"));
    parquetDir = dir.resolve(path).resolve(dataset).resolve(processingLevel).resolve("CTDO2021.parquet");
    Files.createDirectories(parquetDir);
    Files.createFile(parquetDir.resolve("_SUCCESS"));
    parquetDir = dir.resolve(path).resolve(dataset).resolve(processingLevel).resolve("CTDO1900.parquet");
    Files.createDirectories(parquetDir);
    Files.createFile(parquetDir.resolve("foo"));
    assertEquals(Arrays.asList(1900, 2021, 2022) ,YearResolver.resolveYears(null, null, FileSystemType.local, dir.toString(), path, dataset, processingLevel));
  }
}