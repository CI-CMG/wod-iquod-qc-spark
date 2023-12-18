package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.util.Objects;

public class DepthAtPosition {

  private final int lonIndex;
  private final int latIndex;
  private final float depth;
  private final float longitude;
  private final float latitude;
  private double distanceM;

  public DepthAtPosition(int lonIndex, int latIndex, float depth, float longitude, float latitude) {
    this.lonIndex = lonIndex;
    this.latIndex = latIndex;
    this.depth = depth;
    this.longitude = longitude;
    this.latitude = latitude;
  }

  public int getLonIndex() {
    return lonIndex;
  }

  public int getLatIndex() {
    return latIndex;
  }

  public float getDepth() {
    return depth;
  }

  public float getLongitude() {
    return longitude;
  }

  public float getLatitude() {
    return latitude;
  }

  public double getDistanceM() {
    return distanceM;
  }

  public DepthAtPosition setDistanceM(double distanceM) {
    this.distanceM = distanceM;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DepthAtPosition that = (DepthAtPosition) o;
    return lonIndex == that.lonIndex && latIndex == that.latIndex && Float.compare(that.depth, depth) == 0
        && Float.compare(that.longitude, longitude) == 0 && Float.compare(that.latitude, latitude) == 0
        && Double.compare(that.distanceM, distanceM) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lonIndex, latIndex, depth, longitude, latitude, distanceM);
  }

  @Override
  public String toString() {
    return "DepthAtPosition{" +
        "lonIndex=" + lonIndex +
        ", latIndex=" + latIndex +
        ", depth=" + depth +
        ", longitude=" + longitude +
        ", latitude=" + latitude +
        ", distanceM=" + distanceM +
        '}';
  }
}
