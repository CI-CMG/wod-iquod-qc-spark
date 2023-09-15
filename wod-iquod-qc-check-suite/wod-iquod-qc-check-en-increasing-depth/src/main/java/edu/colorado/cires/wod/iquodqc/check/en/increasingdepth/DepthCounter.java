package edu.colorado.cires.wod.iquodqc.check.en.increasingdepth;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.util.Precision;
import org.jetbrains.annotations.NotNull;

public class DepthCounter {

  private final Map<DepthCount, DepthCount> counters = new LinkedHashMap<>();

  public void increment(double temperature) {
    DepthCount nextCounter = new DepthCount(temperature);
    DepthCount existingCounter = counters.get(nextCounter);
    if (existingCounter == null) {
      counters.put(nextCounter, nextCounter);
    } else {
      existingCounter.increment();
    }
  }

  public List<DepthCount> getCounts() {
    return new ArrayList<>(counters.values());
  }

  public static class DepthCount implements Comparable<DepthCount> {

    private final double depth;
    private int count = 1;

    public DepthCount(double depth) {
      this.depth = depth;
    }

    public void increment() {
      count++;
    }

    public int getCount() {
      return count;
    }

    @Override
    public int compareTo(@NotNull DepthCounter.DepthCount o) {
      if (Precision.equals(o.depth, depth)) {
        return 0;
      }
      return Double.compare(o.depth, depth);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DepthCount that = (DepthCount) o;
      return Precision.equals(that.depth, depth);
    }

    @Override
    public int hashCode() {
      // use a constant hash code to maintain the hash code contract with slight variances in equality in floating point numbers
      return 1;
    }
  }

}
