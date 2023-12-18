package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.nio.file.Path;

public class EtopoDataHolder {

  private float[] depths;
  private float[] latitudes;
  private float[] longitudes;
  private Path ncFile;


  public float[] getDepths() {
    return depths;
  }

  public EtopoDataHolder setDepths(float[] depths) {
    this.depths = depths;
    return this;
  }

  public float[] getLatitudes() {
    return latitudes;
  }

  public EtopoDataHolder setLatitudes(float[] latitudes) {
    this.latitudes = latitudes;
    return this;
  }

  public float[] getLongitudes() {
    return longitudes;
  }

  public EtopoDataHolder setLongitudes(float[] longitudes) {
    this.longitudes = longitudes;
    return this;
  }

  public Path getNcFile() {
    return ncFile;
  }

  public EtopoDataHolder setNcFile(Path ncFile) {
    this.ncFile = ncFile;
    return this;
  }
}
