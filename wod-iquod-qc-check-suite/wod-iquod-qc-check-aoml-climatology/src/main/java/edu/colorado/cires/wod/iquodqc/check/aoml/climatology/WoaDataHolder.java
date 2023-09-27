package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import java.nio.file.Path;

public class WoaDataHolder {

  private float time;
  private float[] depths;
  private float[] latitudes;
  private float[] longitudes;
  private Path ncFile;

  public float getTime() {
    return time;
  }

  public WoaDataHolder setTime(float time) {
    this.time = time;
    return this;
  }

  public float[] getDepths() {
    return depths;
  }

  public WoaDataHolder setDepths(float[] depths) {
    this.depths = depths;
    return this;
  }

  public float[] getLatitudes() {
    return latitudes;
  }

  public WoaDataHolder setLatitudes(float[] latitudes) {
    this.latitudes = latitudes;
    return this;
  }

  public float[] getLongitudes() {
    return longitudes;
  }

  public WoaDataHolder setLongitudes(float[] longitudes) {
    this.longitudes = longitudes;
    return this;
  }

  public Path getNcFile() {
    return ncFile;
  }

  public WoaDataHolder setNcFile(Path ncFile) {
    this.ncFile = ncFile;
    return this;
  }
}
