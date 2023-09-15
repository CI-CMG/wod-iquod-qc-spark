package edu.colorado.cires.wod.iquodqc.check.en.constant;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.util.Precision;
import org.jetbrains.annotations.NotNull;

public class TemperatureCounter {

  private final Map<TemperatureCount, TemperatureCount> counters = new LinkedHashMap<>();

  public void increment(double temperature) {
    TemperatureCount nextCounter = new TemperatureCount(temperature);
    TemperatureCount existingCounter = counters.get(nextCounter);
    if (existingCounter == null) {
      counters.put(nextCounter, nextCounter);
    } else {
      existingCounter.increment();
    }
  }

  public List<TemperatureCount> getCounts() {
    return new ArrayList<>(counters.values());
  }

  public static class TemperatureCount implements Comparable<TemperatureCount> {

    private final double temperature;
    private int count = 1;

    public TemperatureCount(double temperature) {
      this.temperature = temperature;
    }

    public void increment() {
      count++;
    }

    public int getCount() {
      return count;
    }

    @Override
    public int compareTo(@NotNull TemperatureCounter.TemperatureCount o) {
      if (Precision.equals(o.temperature, temperature)) {
        return 0;
      }
      return Double.compare(o.temperature, temperature);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TemperatureCount that = (TemperatureCount) o;
      return Precision.equals(that.temperature, temperature);
    }

    @Override
    public int hashCode() {
      // use a constant hash code to maintain the hash code contract with slight variances in equality in floating point numbers
      return 1;
    }
  }

}
