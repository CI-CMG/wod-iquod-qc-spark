package edu.colorado.cires.wod.iquodqc.common.en;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class EnBackgroundCheckerResult {

  private Set<Integer> failures = new LinkedHashSet<>();
  private List<EnBackgroundCheckerLevelResult> levels = new ArrayList<>();

  public Set<Integer> getFailures() {
    return failures;
  }

  public void setFailures(Set<Integer> failures) {
    this.failures = failures;
  }

  public List<EnBackgroundCheckerLevelResult> getLevels() {
    return levels;
  }

}
