package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import com.github.davidmoten.geo.GeoHash;
import java.util.Set;
import java.util.TreeSet;
import org.geotools.referencing.CRS;
import org.geotools.referencing.GeodeticCalculator;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class GeoHashFinder {

  private static final int GEOHASH_LENGTH = 3;
  private static final CoordinateReferenceSystem EPSG_4326;

  static {
    try {
      EPSG_4326 = CRS.decode("EPSG:4326");
    } catch (FactoryException e) {
      throw new RuntimeException("Unable to determine CRS", e);
    }
  }

  private static final double NORTH = 0D;
  private static final double EAST = 90D;
  private static final double SOUTH = 180D;
  private static final double WEST = 270D;

  private static double distanceToPole(double longitude, double latitude, boolean north) {
    GeodeticCalculator gc = new GeodeticCalculator(EPSG_4326);
    gc.setStartingGeographicPoint(longitude, latitude);
    gc.setDestinationGeographicPoint(longitude, north ? 90D : -90D);
    return gc.getOrthodromicDistance();
  }

  private static double maxLongitude(double latitude) {
    GeodeticCalculator gc = new GeodeticCalculator(EPSG_4326);
    gc.setStartingGeographicPoint(0D, latitude);
    gc.setDestinationGeographicPoint(180D, latitude);
    return gc.getOrthodromicDistance() * 2D;
  }

  private static double north(double longitude, double latitude, double maxDistanceM) {
    GeodeticCalculator gc = new GeodeticCalculator(EPSG_4326);
    gc.setStartingGeographicPoint(longitude, latitude);
    gc.setDirection(NORTH, maxDistanceM);
    return gc.getDestinationGeographicPoint().getY();
  }

  private static double south(double longitude, double latitude, double maxDistanceM) {
    GeodeticCalculator gc = new GeodeticCalculator(EPSG_4326);
    gc.setStartingGeographicPoint(longitude, latitude);
    gc.setDirection(SOUTH, maxDistanceM);
    return gc.getDestinationGeographicPoint().getY();
  }

  private static double east(double longitude, double latitude, double maxDistanceM) {
    GeodeticCalculator gc = new GeodeticCalculator(EPSG_4326);
    gc.setStartingGeographicPoint(longitude, latitude);
    gc.setDirection(EAST, maxDistanceM);
    return gc.getDestinationGeographicPoint().getX();
  }

  private static double west(double longitude, double latitude, double maxDistanceM) {
    GeodeticCalculator gc = new GeodeticCalculator(EPSG_4326);
    gc.setStartingGeographicPoint(longitude, latitude);
    gc.setDirection(WEST, maxDistanceM);
    return gc.getDestinationGeographicPoint().getX();
  }

  public static Set<String> getNeighborsInDistance(double longitude, double latitude, double maxDistanceM) {

    Set<String> hashes = new TreeSet<>();

    double distanceToN = distanceToPole(longitude, latitude, true);
    double distanceToS = distanceToPole(longitude, latitude, false);
    double maxLongitude = maxLongitude(latitude);

    double north = north(longitude, latitude, Math.min(maxDistanceM, distanceToN));
    double south = south(longitude, latitude, Math.min(maxDistanceM, distanceToS));
    double east = maxDistanceM >= maxLongitude ? 180D : east(longitude, latitude, maxDistanceM);
    double west = maxDistanceM >= maxLongitude ? -180D : west(longitude, latitude, maxDistanceM);

    if (longitude < 0 && west > 0 || longitude > 0 && east < 0) {
      hashes.addAll(GeoHash.coverBoundingBox(north, west, south, 180D, GEOHASH_LENGTH).getHashes());
      hashes.addAll(GeoHash.coverBoundingBox(north, -180D, south, east, GEOHASH_LENGTH).getHashes());
    } else {
      hashes.addAll(GeoHash.coverBoundingBox(north, west, south, east, GEOHASH_LENGTH).getHashes());
    }

    return hashes;
  }

}
