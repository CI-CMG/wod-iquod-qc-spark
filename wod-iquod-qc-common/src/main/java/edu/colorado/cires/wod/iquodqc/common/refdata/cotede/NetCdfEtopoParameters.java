package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class NetCdfEtopoParameters implements EtopoDataAccessor {

  private static final String LAT_FIELD_NAME = "ETOPO05_Y";
  private static final String LON_FIELD_NAME = "ETOPO05_X";
  private static final String ELEVATIONS_FIELD_NAME = "ROSE";

  private final double[] longitudes;
  private final double[] latitudes;
  private final Path netCdfPath;

  public NetCdfEtopoParameters(Path netCdfPath) {
    this.netCdfPath = netCdfPath;
    try (NetcdfFile nc = NetcdfFiles.open(netCdfPath.toString())) {
      longitudes = (double[]) Objects.requireNonNull(nc.findVariable(LON_FIELD_NAME)).read().copyTo1DJavaArray();
      for (int i = 0; i < longitudes.length; i++) {
        if (longitudes[i] > 180D) {
          longitudes[i] = longitudes[i] - 360D;
        }
      }
      latitudes = (double[]) Objects.requireNonNull(nc.findVariable(LAT_FIELD_NAME)).read().copyTo1DJavaArray();
    } catch (IOException e) {
      throw new RuntimeException("Unable to open NetCDF file", e);
    }
  }

  @Override
  public double[] getLongitudes() {
    return longitudes;
  }

  @Override
  public double[] getLatitudes() {
    return latitudes;
  }

  @Override
  public float[][] getDepths(int minIndexLat, int maxIndexLat, int minIndexLon, int maxIndexLon) {
    try (NetcdfFile nc = NetcdfFiles.open(netCdfPath.toString())) {
      Variable rose = Objects.requireNonNull(nc.findVariable(ELEVATIONS_FIELD_NAME));
      int lonSize = maxIndexLon - minIndexLon + 1;
      int latSize = maxIndexLat - minIndexLat + 1;
      float[][] result = new float[lonSize][latSize];
      int resultLonIndex = 0;
      for (int lonI = minIndexLon; lonI <= maxIndexLon; lonI++) {
        int resultLatIndex = 0;
        for (int latI = minIndexLat; latI <= maxIndexLat; latI++) {
          int netCdfLon;
          int netCdfLat = latI;
          if (lonI < 0) {
            netCdfLon = longitudes.length + lonI;
          } else if (lonI >= longitudes.length) {
            netCdfLon = lonI - longitudes.length;
          } else {
            netCdfLon = lonI;
          }
          result[resultLatIndex][resultLonIndex] = rose.read(new int[]{netCdfLat, netCdfLon}, new int[]{1, 1}).getFloat(0);
          resultLatIndex++;
        }
        resultLonIndex++;
      }
      return result;
    } catch (InvalidRangeException | IOException e) {
      throw new RuntimeException("Unable to read NetCdf data", e);
    }
  }
}
