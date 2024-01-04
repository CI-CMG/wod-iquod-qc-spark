package edu.colorado.cires.wod.iquodqc.check.cotede.constantclustersize;

import org.apache.commons.math3.util.FastMath;

public class ConstantClusterSize {

  public static int[] computeClusterSizes(double[] input, double tolerance) {
    int inputLength = input.length;
    int[] output = new int[inputLength];

    tolerance = tolerance  + (1e-5 * tolerance);

    for (int i = 0; i < inputLength; i++) {
      int sum = 0;

      double currentValue = input[i];

      if (Double.isNaN(currentValue)) {
        continue;
      }

      for (int j = i - 1; j >= 0; j--) {
        double previousValue = input[j];
        if (Double.isNaN(previousValue)) {
          break;
        }
        if (FastMath.abs(previousValue - currentValue) <= tolerance) {
          sum += 1;
        } else {
          break;
        }
      }

      for (int j = i + 1; j < inputLength; j++) {
        double nextValue = input[j];
        if (Double.isNaN(nextValue)) {
          break;
        }
        if (FastMath.abs(nextValue - currentValue) <= tolerance) {
          sum += 1;
        } else {
          break;
        }
      }

      output[i] = sum;
    }

    return output;
  }

}
