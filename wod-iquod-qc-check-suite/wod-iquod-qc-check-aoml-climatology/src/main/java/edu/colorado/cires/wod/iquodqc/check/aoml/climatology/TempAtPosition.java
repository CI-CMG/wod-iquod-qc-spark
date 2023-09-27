package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import java.util.Objects;

class TempAtPosition {

  private final int depthIndex;
  private final int lonIndex;
  private final int latIndex;
  private final float depth;
  private final float longitude;
  private final float latitude;
  private final float temperature;
  private double distanceM;

  TempAtPosition(int depthIndex, int lonIndex, int latIndex, float depth, float longitude, float latitude, float temperature) {
    this.depthIndex = depthIndex;
    this.lonIndex = lonIndex;
    this.latIndex = latIndex;
    this.depth = depth;
    this.longitude = longitude;
    this.latitude = latitude;
    this.temperature = temperature;
  }

  public int getDepthIndex() {
    return depthIndex;
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

  public float getTemperature() {
    return temperature;
  }

  public double getDistanceM() {
    return distanceM;
  }

  public TempAtPosition setDistanceM(double distanceM) {
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
    TempAtPosition that = (TempAtPosition) o;
    return depthIndex == that.depthIndex && lonIndex == that.lonIndex && latIndex == that.latIndex && Float.compare(that.depth, depth) == 0
        && Float.compare(that.longitude, longitude) == 0 && Float.compare(that.latitude, latitude) == 0
        && Float.compare(that.temperature, temperature) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(depthIndex, lonIndex, latIndex, depth, longitude, latitude, temperature);
  }

  @Override
  public String toString() {
    return "TempAtPosition{" +
        "depthIndex=" + depthIndex +
        ", lonIndex=" + lonIndex +
        ", latIndex=" + latIndex +
        ", depth=" + depth +
        ", longitude=" + longitude +
        ", latitude=" + latitude +
        ", temperature=" + temperature +
        '}';
  }
}
