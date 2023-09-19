package edu.colorado.cires.wod.iquodqc.common.en;

public class EnBackgroundCheckerLevelResult {

  private final int origLevel;
  private final double ptLevel;
  private final double bgLevel;


  public EnBackgroundCheckerLevelResult(int origLevel, double ptLevel, double bgLevel) {
    this.origLevel = origLevel;
    this.ptLevel = ptLevel;
    this.bgLevel = bgLevel;
  }

  public int getOrigLevel() {
    return origLevel;
  }

  public double getPtLevel() {
    return ptLevel;
  }

  public double getBgLevel() {
    return bgLevel;
  }


}
