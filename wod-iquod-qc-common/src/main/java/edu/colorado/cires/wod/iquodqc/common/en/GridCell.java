package edu.colorado.cires.wod.iquodqc.common.en;

public class GridCell {

  private final int iLon;
  private final int iLat;

  public GridCell(int iLon, int iLat) {
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
