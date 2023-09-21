package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import java.util.ArrayList;
import java.util.List;

class StdLevel {

  private final int stdLevelIndex;
  private double stdLevel;
  private final List<StdLevelWrapper> levelWrappers = new ArrayList<>();
  private Double pge;

  public StdLevel(int stdLevelIndex) {
    this.stdLevelIndex = stdLevelIndex;
  }

  public int getStdLevelIndex() {
    return stdLevelIndex;
  }

  public double getStdLevel() {
    return stdLevel;
  }

  public void finalizeStdLevel() {
    stdLevel = levelWrappers.stream().mapToDouble(StdLevelWrapper::getDiffLevel).sum() / (double) levelWrappers.size();
  }

  public List<StdLevelWrapper> getLevelWrappers() {
    return levelWrappers;
  }

  public Double getPge() {
    return pge;
  }

  public void setPge(double pge) {
    this.pge = pge;
  }

}
