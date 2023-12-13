package edu.colorado.cires.wod.iquodqc.check.cotede.globalrange;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CoTeDeGlobalRange {
  
  public static List<Integer> checkGlobalRange(double[] input, double min, double max) {
    return IntStream.range(0, input.length).boxed()
        .filter(i -> {
          double value = input[i];
          return value < min || value > max || !Double.isFinite(value);
        })
        .collect(Collectors.toList());
  }

}
