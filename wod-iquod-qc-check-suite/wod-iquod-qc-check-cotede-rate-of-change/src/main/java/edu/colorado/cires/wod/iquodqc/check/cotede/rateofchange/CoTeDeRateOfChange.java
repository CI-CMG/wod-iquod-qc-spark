package edu.colorado.cires.wod.iquodqc.check.cotede.rateofchange;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CoTeDeRateOfChange {

  public static double[] computeRateOfChange(double[] input) {
    int inputLength = input.length;

    double[] output = new double[inputLength];
    Arrays.fill(output, Double.NaN);

    for (int i = 1; i < inputLength; i++) {
      output[i] = input[i] - input[i - 1];
    }
    
    return output;
  }
  
  public static Collection<Integer> getFlags(double[] input, double[] rateOfChange, double threshold) {
    return IntStream.range(0, input.length).boxed()
        .filter(i -> {
          boolean inputWasInvalid = Double.isNaN(input[i]) || !Double.isFinite(input[i]);
          if (i == 0) {
            return inputWasInvalid;
          }
          if (inputWasInvalid) {
            return true;
          }
          double value = rateOfChange[i];
          if (Double.isNaN(value) || !Double.isFinite(value)) {
            return false;
          }
          return Math.abs(value) > threshold;
        }).collect(Collectors.toList());
  }
}
