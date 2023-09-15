package edu.colorado.cires.wod.iquodqc.common.refdata.en;

import ucar.ma2.Array;

public class EnBgCheckInfoParameters {

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

  EnBgCheckInfoParameters() {

  }

  public double getFillValue() {
    return fillValue;
  }

  void setFillValue(double fillValue) {
    this.fillValue = fillValue;
  }

  public double getLonGridSize() {
    return lonGridSize;
  }

  void setLonGridSize(double lonGridSize) {
    this.lonGridSize = lonGridSize;
  }

  public double getLatGridSize() {
    return latGridSize;
  }

  void setLatGridSize(double latGridSize) {
    this.latGridSize = latGridSize;
  }

  public Array getLon() {
    return lon;
  }

  void setLon(Array lon) {
    this.lon = lon;
  }

  public Array getLat() {
    return lat;
  }

  void setLat(Array lat) {
    this.lat = lat;
  }

  public Array getDepth() {
    return depth;
  }

  void setDepth(Array depth) {
    this.depth = depth;
  }

  public Array getMonth() {
    return month;
  }

  void setMonth(Array month) {
    this.month = month;
  }

  public Array getClim() {
    return clim;
  }

  void setClim(Array clim) {
    this.clim = clim;
  }

  public Array getBgev() {
    return bgev;
  }

  void setBgev(Array bgev) {
    this.bgev = bgev;
  }

  public Array getObev() {
    return obev;
  }

  void setObev(Array obev) {
    this.obev = obev;
  }
}
