package edu.colorado.cires.wod.iquodqc.common.refdata.en;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.*;

import edu.colorado.cires.wod.iquodqc.common.en.EnUtils;
import edu.colorado.cires.wod.iquodqc.common.en.GridCell;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;
import ucar.nc2.write.NetcdfFormatWriter;

public class EnBgCheckInfoParametersReaderTest {
  private static final Path DIR = Paths.get("target/nctest");

  @BeforeEach
  public void before() throws Exception {
    Files.createDirectories(DIR);
  }

  @AfterEach
  public void after() throws Exception {
    FileUtils.deleteQuietly(DIR.toFile());
  }

  private static String createTestNetCdf(float[] testLon, float[] testLat) throws Exception {
    Path ncFile = DIR.resolve("test.nc");

    NetcdfFormatWriter.Builder builder = NetcdfFormatWriter.createNewNetcdf3(ncFile.toString());
    Dimension latDim = builder.addDimension("lat", testLat.length);
    Dimension lonDim = builder.addDimension("lon", testLon.length);
    Dimension zDim = builder.addDimension("z", 1);
    Dimension monDim = builder.addDimension("mon", 1);
    builder.addVariable("longitude", DataType.FLOAT, Arrays.asList(lonDim));
    builder.addVariable("latitude", DataType.FLOAT, Arrays.asList(latDim));
    builder.addVariable("depth", DataType.FLOAT, Arrays.asList(zDim));
    builder.addVariable("month", DataType.SHORT, Arrays.asList(monDim));
    Variable.Builder potmBuilder = builder.addVariable("potm_climatology", DataType.FLOAT, Arrays.asList(zDim, latDim, lonDim, monDim));
    potmBuilder.addAttribute(new Attribute("_FillValue", Float.MIN_VALUE));
    builder.addVariable("bg_err_var", DataType.FLOAT, Arrays.asList(zDim, latDim, lonDim));
    builder.addVariable("ob_err_var", DataType.FLOAT, Arrays.asList(zDim));

    try (NetcdfFormatWriter writer = builder.build()) {
      writer.write("longitude", Array.makeFromJavaArray(testLon));
      writer.write("latitude", Array.makeFromJavaArray(testLat));
      writer.write("depth", Array.makeFromJavaArray(new float[1]));
      writer.write("month", Array.makeFromJavaArray(new short[1]));
      writer.write("potm_climatology", Array.factory(DataType.FLOAT, writer.findVariable("potm_climatology").getShape(), new float[testLat.length * testLon.length]));
      writer.write("bg_err_var", Array.factory(DataType.FLOAT, writer.findVariable("bg_err_var").getShape(), new float[testLat.length * testLon.length]));
      writer.write("ob_err_var", Array.makeFromJavaArray(new float[1]));
    }

    return ncFile.toString();
  }

  @Test
  public void testEnBackgroundCheckFindGridCell() throws Exception {
    Cast cast = Cast.builder()
        .withLatitude(121D)
        .withLongitude(421D)
        .withCastNumber(123)
        .withDepths(Collections.singletonList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build()
        ))
        .build();

    float[] testLon = new float[]{0F, 60F, 120F, 180F, 240F, 300F};
    float[] testLat = new float[]{-90F, -60F, -30F, 0F, 30F, 60F, 90F};

    Properties properties = new Properties();
    properties.put("EN_bgcheck_info.netcdf.uri", createTestNetCdf(testLon, testLat));
    EnBgCheckInfoParameters parameters = EnBgCheckInfoParametersReader.loadParameters(properties);
    GridCell gridCell = EnUtils.findGridCell(cast, parameters);
    assertTrue(Precision.equals(1D, gridCell.getiLon()));
    assertTrue(Precision.equals(5D, gridCell.getiLat()));
  }

  @Test
  public void testEnBackgroundCheckFindGridCellEvenSpacing() {
    /*
        findGridCell will silently fail if grid spacings are not even;
        check that asserts are raised checking for this.
     */

    Cast cast = Cast.builder()
        .withLatitude(29D)
        .withLongitude(51D)
        .withCastNumber(123)
        .withDepths(Collections.singletonList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build()
        ))
        .build();

    assertThrows(IllegalArgumentException.class, () -> {

      float[] testLon = new float[]{10F, 20F, 30F, 40F};
      float[] testLat = new float[]{10F, 21F, 28F, 40F};

      Properties properties = new Properties();
      properties.put("EN_bgcheck_info.netcdf.uri", createTestNetCdf(testLon, testLat));
      EnBgCheckInfoParameters parameters = EnBgCheckInfoParametersReader.loadParameters(properties);
      EnUtils.findGridCell(cast, parameters);
    });

  }

  @Test
  public void testEnBackgroundCheckFindGridCellEvenSpacing2() {
    /*
        findGridCell will silently fail if grid spacings are not even;
        check that asserts are raised checking for this.
     */

    Cast cast = Cast.builder()
        .withLatitude(29D)
        .withLongitude(51D)
        .withCastNumber(123)
        .withDepths(Collections.singletonList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D)
                    .build()))
                .build()
        ))
        .build();

    assertThrows(IllegalArgumentException.class, () -> {

      float[] testLon = new float[]{10F, 21F, 28F, 40F};
      float[] testLat = new float[]{10F, 20F, 30F, 40F};

      Properties properties = new Properties();
      properties.put("EN_bgcheck_info.netcdf.uri", createTestNetCdf(testLon, testLat));
      EnBgCheckInfoParameters parameters = EnBgCheckInfoParametersReader.loadParameters(properties);
      EnUtils.findGridCell(cast, parameters);
    });

  }
}