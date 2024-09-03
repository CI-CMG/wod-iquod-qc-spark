package edu.colorado.cires.wod.spark.iquodqc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

public class OsPoolDagGeneratorTest {

  @Test
  public void testRun() throws Exception {
    Path outputFile = Paths.get("target/test.dag");
    Path listFile = Paths.get("src/test/resources/dataset-year-list.txt");
    OsPoolDagGenerator osPoolDagGenerator = new OsPoolDagGenerator();
    osPoolDagGenerator.setListFile(listFile);
    osPoolDagGenerator.setOutputFile(outputFile);
    osPoolDagGenerator.setOsdfPrefix("osdf:///ospool/apXX/data/<username>/iquod-qc/2024-08");
    osPoolDagGenerator.setDateFolder("2024-08");

    osPoolDagGenerator.run();
    assertEquals(
        FileUtils.readFileToString(new File("src/test/resources/test.dag"), StandardCharsets.UTF_8),
        FileUtils.readFileToString(outputFile.toFile(), StandardCharsets.UTF_8)
        );
  }
}