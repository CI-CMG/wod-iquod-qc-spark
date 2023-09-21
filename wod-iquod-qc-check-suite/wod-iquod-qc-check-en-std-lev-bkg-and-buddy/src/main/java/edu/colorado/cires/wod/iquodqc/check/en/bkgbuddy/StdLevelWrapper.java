package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundCheckerLevelResult;

class StdLevelWrapper {

  private final EnBackgroundCheckerLevelResult level;
  private final int stdLevelIndex;
  private final double stdLevelDifference;
  private final double diffLevel;

  StdLevelWrapper(EnBackgroundCheckerLevelResult level, int stdLevelIndex, double stdLevelDifference, double diffLevel) {
    this.level = level;
    this.stdLevelIndex = stdLevelIndex;
    this.stdLevelDifference = stdLevelDifference;
    this.diffLevel = diffLevel;
  }

  EnBackgroundCheckerLevelResult getLevel() {
    return level;
  }

  int getStdLevelIndex() {
    return stdLevelIndex;
  }

  double getStdLevelDifference() {
    return stdLevelDifference;
  }

  double getDiffLevel() {
    return diffLevel;
  }
}
