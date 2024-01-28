package edu.colorado.cires.wod.iquodqc.check.cotede.rateofchange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
    List<Integer> flags = new ArrayList<>(0);
    for (int i = 1; i < input.length; i++) {
      boolean inputWasInvalid = Double.isNaN(input[i]) || !Double.isFinite(input[i]);
      if (!inputWasInvalid) {
        double value = rateOfChange[i];
        boolean valueIsInvalid = Double.isNaN(value) || !Double.isFinite(value);
        if (!valueIsInvalid && Math.abs(value) > threshold) {
          flags.add(i);
        }
      }
    }
    return flags;
  }
}
