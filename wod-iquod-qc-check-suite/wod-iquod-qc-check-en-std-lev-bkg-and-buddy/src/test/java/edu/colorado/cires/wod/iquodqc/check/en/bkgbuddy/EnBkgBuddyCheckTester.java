package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.List;
import java.util.TreeMap;

public class EnBkgBuddyCheckTester extends EnBkgBuddyCheck {


  @Override
  protected void reinstateLevels(Cast cast, TreeMap<Integer, StdLevel> pgeLevels, List<Double> bgsl, List<Double> slev) {
    // no-op
  }

}
