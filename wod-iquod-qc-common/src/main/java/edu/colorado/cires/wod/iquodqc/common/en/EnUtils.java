package edu.colorado.cires.wod.iquodqc.common.en;

import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParameters;
import edu.colorado.cires.wod.parquet.model.Cast;
import ucar.ma2.Array;

public class EnUtils {

  public static GridCell findGridCell(Cast cast, EnBgCheckInfoParameters parameters) {

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

  private static int normalizeLatitude(double lat) {
    double latitude = lat * Math.PI / 180;
    return (int) Math.round(Math.asin(Math.sin(latitude)) / (Math.PI / 180D));
  }

  // how Python does mod with negative numbers
  private static long mod(long a, long b) {
    long c = a % b;
    return (c < 0) ? c + b : c;
  }

}
