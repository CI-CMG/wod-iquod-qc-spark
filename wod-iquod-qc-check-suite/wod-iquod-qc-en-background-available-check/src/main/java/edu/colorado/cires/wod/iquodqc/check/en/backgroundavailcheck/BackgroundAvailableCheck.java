package edu.colorado.cires.wod.iquodqc.check.en.backgroundavailcheck;

import static edu.colorado.cires.wod.iquodqc.common.DepthUtils.getTemperature;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.ArrayUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.DoubleStream;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

public class BackgroundAvailableCheck extends CommonCastCheck {

  private final static String NAME = "EN_background_available_check";
  private final static String EN_BG_AVAIL_CHECK_NC_PROP = NAME + ".netcdf.uri";
  private final static int CONNECT_TIMEOUT = 2000;
  private final static int READ_TIMEOUT = 1000 * 60 * 15;
  private static final double THRESHOLD = 0.0000000001;

  private static EnBackgroundCheckParameters parameters;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Dataset<CastCheckResult> joinResultDataset(CastCheckContext context) {
    if (parameters == null) {
      loadParameters(context);
    }
    return super.joinResultDataset(context);
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {

    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();

    GridCell gridCell = findGridCell(cast);
    int month = cast.getMonth() - 1;
    double[] clim;
    try {
      clim = (double[]) parameters.getClim()
          .section(Arrays.asList(
              null,
              Range.make(gridCell.getiLat(), gridCell.getiLat()),
              Range.make(gridCell.getiLon(), gridCell.getiLon()),
              Range.make(month, month)
          )).get1DJavaArray(DataType.DOUBLE);
    } catch (InvalidRangeException e) {
      throw new RuntimeException(e);
    }

    boolean[] mask = ArrayUtils.getMask(clim, parameters.getFillValue());

    double[] ncDepths = (double[]) parameters.getDepth().get1DJavaArray(DataType.DOUBLE);

    clim = ArrayUtils.mask(clim, mask);
    if (clim.length == 0) {
      for (int i = 0; i < depths.size(); i++) {
        failures.add(i);
      }
      return failures;
    }

    ncDepths = ArrayUtils.mask(ncDepths, mask);

    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      Optional<ProfileData> maybeTemp = getTemperature(depth);

      if (maybeTemp.isPresent() && interpolate(depth.getDepth(), ncDepths, clim)) {
        failures.add(i);
      }

    }

    return failures;
  }

  private static void loadParameters(CastCheckContext context) {
    synchronized (BackgroundAvailableCheck.class) {
      if (parameters == null) {
        String uri = context.getProperties().getProperty(EN_BG_AVAIL_CHECK_NC_PROP);
        Path ncFile;
        try {
          ncFile = Files.createTempFile(NAME + "_", ".nc");
        } catch (IOException e) {
          throw new RuntimeException("Unable to create temp file", e);
        }
        try {
          FileUtils.copyURLToFile(
              new URL(uri),
              ncFile.toFile(),
              CONNECT_TIMEOUT,
              READ_TIMEOUT);

          parameters = readEnBackgroundCheckAux(ncFile);
          validateGrid();

        } catch (IOException e) {
          throw new RuntimeException("Unable to download " + uri, e);
        } finally {
          FileUtils.deleteQuietly(ncFile.toFile());
        }
      }
    }
  }

  private static class EnBackgroundCheckParameters {

    private Array lon;
    private Array lat;
    private Array depth;
    private Array month;
    private Array clim;
    private Array bgev;
    private Array obev;
    private double lonGridSize;
    private double latGridSize;
    private double fillValue;

    public double getFillValue() {
      return fillValue;
    }

    public void setFillValue(double fillValue) {
      this.fillValue = fillValue;
    }

    public double getLonGridSize() {
      return lonGridSize;
    }

    public void setLonGridSize(double lonGridSize) {
      this.lonGridSize = lonGridSize;
    }

    public double getLatGridSize() {
      return latGridSize;
    }

    public void setLatGridSize(double latGridSize) {
      this.latGridSize = latGridSize;
    }

    public Array getLon() {
      return lon;
    }

    public void setLon(Array lon) {
      this.lon = lon;
    }

    public Array getLat() {
      return lat;
    }

    public void setLat(Array lat) {
      this.lat = lat;
    }

    public Array getDepth() {
      return depth;
    }

    public void setDepth(Array depth) {
      this.depth = depth;
    }

    public Array getMonth() {
      return month;
    }

    public void setMonth(Array month) {
      this.month = month;
    }

    public Array getClim() {
      return clim;
    }

    public void setClim(Array clim) {
      this.clim = clim;
    }

    public Array getBgev() {
      return bgev;
    }

    public void setBgev(Array bgev) {
      this.bgev = bgev;
    }

    public Array getObev() {
      return obev;
    }

    public void setObev(Array obev) {
      this.obev = obev;
    }
  }

  private static EnBackgroundCheckParameters readEnBackgroundCheckAux(Path bgcheckInfoFile) throws IOException {
    try (NetcdfFile nc = NetcdfFiles.open(bgcheckInfoFile.toString())) {
      EnBackgroundCheckParameters parameters = new EnBackgroundCheckParameters();
      parameters.setLon(Objects.requireNonNull(nc.findVariable("longitude")).read());
      parameters.setLat(Objects.requireNonNull(nc.findVariable("latitude")).read());
      parameters.setDepth(Objects.requireNonNull(nc.findVariable("depth")).read());
      parameters.setMonth(Objects.requireNonNull(nc.findVariable("month")).read());
      parameters.setClim(Objects.requireNonNull(nc.findVariable("potm_climatology")).read());
      parameters.setFillValue(nc.findVariable("potm_climatology").findAttribute("_FillValue").getNumericValue().doubleValue());
      //TODO are these needed, fix setter
//      parameters.setLon(Objects.requireNonNull(nc.findVariable("bgev")).read());
//      parameters.setLon(Objects.requireNonNull(nc.findVariable("obev")).read());
      return parameters;
    }
  }



  private static void validateGrid() {
    double lonSize = parameters.getLon().getDouble(1) - parameters.getLon().getDouble(0);
    double latSize = parameters.getLat().getDouble(1) - parameters.getLat().getDouble(0);
    for (int i = 1; i < parameters.getLon().getSize(); i++) {
      double size = parameters.getLon().getDouble(i) - parameters.getLon().getDouble(i - 1);
      if (lonSize < 0 || size < 0 || Math.abs(lonSize - size) > THRESHOLD) {
        throw new IllegalArgumentException("Invalid Background Check File: Longitude not evenly spaced");
      }
    }
    for (int i = 1; i < parameters.getLat().getSize(); i++) {
      double size = parameters.getLat().getDouble(i) - parameters.getLat().getDouble(i - 1);
      if (latSize < 0 || size < 0 || Math.abs(latSize - size) > THRESHOLD) {
        throw new IllegalArgumentException("Invalid Background Check File: Latitude not evenly spaced");
      }
    }
    parameters.setLonGridSize(lonSize);
    parameters.setLatGridSize(latSize);
  }

  private static class GridCell {

    private final int iLon;
    private final int iLat;

    private GridCell(int iLon, int iLat) {
      this.iLon = iLon;
      this.iLat = iLat;
    }

    public int getiLon() {
      return iLon;
    }

    public int getiLat() {
      return iLat;
    }
  }

  private static int normalizeLatitude(double lat) {
    double latitude = lat * Math.PI / 180;
    return (int) Math.round(Math.asin(Math.sin(latitude)) / (Math.PI / 180D));
  }

  // how Python does mod with negative numbers
  private static long mod(long a, long b) {
    long c = a % b;
    return (c < 0) ? c + b : c;
  }

  private GridCell findGridCell(Cast cast) {

    double lon = cast.getLongitude();
    Array grid = parameters.getLon();
    long nlon = grid.getSize();
    int ilon = (int) mod(Math.round((lon - grid.getDouble(0)) / (grid.getDouble(1) - grid.getDouble(0))), nlon);

    if (ilon < 0 || ilon >= nlon) {
      throw new IllegalStateException("Longitude is out of range: " + ilon + " " + nlon);
    }

    double lat = cast.getLatitude();
    lat = normalizeLatitude(lat);
    grid = parameters.getLat();
    long nlat = grid.getSize();
    int ilat = (int) Math.round((lat - grid.getDouble(0)) / (grid.getDouble(1) - grid.getDouble(0)));
    // Checks for edge case where lat is ~90.
    if (ilat == nlat) {
      ilat -= 1;
    }

    if (ilat < 0 || ilat >= nlat) {
      throw new IllegalStateException("Latitude is out of range: " + ilat + " " + nlat);
    }

    return new GridCell(ilon, ilat);
  }

  private static boolean interpolate(double z, double[] depths, double[] clim) {

//    LinearInterpolator interp = new LinearInterpolator();
//    PolynomialSplineFunction f = interp.interpolate(depths, clim);
//
//    if (f.isValidPoint(z)) {
//      return OptionalDouble.of(f.value(z));
//    }
//
//    return OptionalDouble.empty();

    /*
    Original Python logic was:

      # Get the climatology and error variance values at this level.
        climLevel = np.interp(z[iLevel], depths, clim, right=99999)
        if climLevel == 99999:
            qc[iLevel] = True # This could reject some good data if the
                              # climatology is incomplete, but also can act as
                              # a check that the depth of the profile is
                              # consistent with the depth of the ocean.

     This seems flawed though since a failure only happens when z > the maximum depth and the interpolated value is
     never used.

     numpy doc:

     right optional float or complex corresponding to fp
     Value to return for x > xp[-1], default is fp[-1].
     */

    return z > DoubleStream.of(depths).max().orElseThrow(() -> new IllegalStateException("depths was empty"));
  }

}
