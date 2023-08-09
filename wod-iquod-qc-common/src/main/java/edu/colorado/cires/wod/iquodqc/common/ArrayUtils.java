package edu.colorado.cires.wod.iquodqc.common;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.commons.math3.util.Precision;
import org.apache.commons.math3.util.ResizableDoubleArray;

public final class ArrayUtils {


  public static double median(double[] values) {
    return new Median().evaluate(values);
  }

  public static double mean(double[] values) {
    return new Mean().evaluate(values);
  }

  public static double[] deleteElement(double[] values, int d) {
    ResizableDoubleArray a = new ResizableDoubleArray();
    for (int i = 0; i < values.length; i++) {
      if (i != d) {
        a.addElement(values[i]);
      }
    }
    return a.getElements();
  }

  public static boolean[] getMask(double[] a, double fillValue) {
    boolean[] mask = new boolean[a.length];
    for (int i = 0; i < a.length; i++) {
      mask[i] = !Precision.equals(a[i], fillValue);
    }
    return mask;
  }

  public static double[] mask(double[] a, boolean[] mask) {
    if (a.length != mask.length) {
      throw new IllegalArgumentException("Array and mask must be of equal length");
    }
    ResizableDoubleArray result = new ResizableDoubleArray();
    for (int i = 0; i < a.length; i++) {
      if (mask[i]) {
        result.addElement(a[i]);
      }
    }
    return result.getElements();
  }

  private ArrayUtils() {

  }
}
