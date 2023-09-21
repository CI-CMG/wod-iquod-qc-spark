package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import java.util.ArrayList;
import java.util.List;

class StdLevel {

  private final int stdLevelIndex;
  private double stdLevel;
//  private double meanDifference;
  private final List<StdLevelWrapper> levelWrappers = new ArrayList<>();
  private double pge;

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
//    meanDifference = levelWrappers.stream().mapToDouble(StdLevelWrapper::getStdLevelDifference).reduce(0D, (a, b) -> a + b / levelWrappers.size());
  }

  public List<StdLevelWrapper> getLevelWrappers() {
    return levelWrappers;
  }

  public double getPge() {
    return pge;
  }

  public StdLevel setPge(double pge) {
    this.pge = pge;
    return this;
  }

//  public double getMeanDifference() {
//    return meanDifference;
//  }
}
