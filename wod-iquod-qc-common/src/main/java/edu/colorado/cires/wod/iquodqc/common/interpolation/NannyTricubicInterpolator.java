package edu.colorado.cires.wod.iquodqc.common.interpolation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.DoubleStream;
import org.apache.commons.math3.analysis.interpolation.TricubicInterpolator;

public class NannyTricubicInterpolator {

  public static class TooManyNansException extends Exception {

    public TooManyNansException(String message) {
      super(message);
    }
  }

  private final double[] x;
  private final double[] y;
  private final double[] z;
  private final double[][][] values;
  private double[][][] deNanValues;


  public NannyTricubicInterpolator(double[] x, double[] y, double[] z, double[][][] values) {
    this.x = x;
    this.y = y;
    this.z = z;
    this.values = values;
  }

  public double interpolate(double x, double y, double z) throws TooManyNansException {
    interpolateHoles();
    return new TricubicInterpolator().interpolate(this.x, this.y, this.z, deNanValues).value(x, y, z);
  }

  private void interpolateHoles() throws TooManyNansException {
    deNanValues = new double[x.length][y.length][z.length];

    List<InterpolationPoint> holes = new ArrayList<>();
    for (int xIndex = 0; xIndex < x.length; xIndex++) {
      for (int yIndex = 0; yIndex < y.length; yIndex++) {
        for (int zIndex = 0; zIndex < z.length; zIndex++) {
          double value = values[xIndex][yIndex][zIndex];
          deNanValues[xIndex][yIndex][zIndex] = value;
          if (Double.isNaN(value)) {
            holes.add(new InterpolationPoint(xIndex, yIndex, zIndex, x[xIndex], y[yIndex], z[zIndex], value));
          }
        }
      }
    }

    // first pass: look for holes that are surrounded, "bubbles"
    removeBubbles(holes);

    // second pass: look for holes that are on the edge of a "bubble"
    removeEdgeBubbles(holes);

    if (!holes.isEmpty()) {
      throw new TooManyNansException("Too many NaNs to complete interpolation");
    }
  }

  private InterpolationPoint getPoint(int xIndex, int yIndex, int zIndex) {
    if (xIndex < 0 || xIndex > x.length || yIndex < 0 || yIndex > y.length || zIndex < 0 || zIndex > z.length) {
      return null;
    }
    double value = deNanValues[xIndex][yIndex][zIndex];
    if (Double.isNaN(value)) {
      return null;
    }
    return new InterpolationPoint(xIndex, yIndex, zIndex, x[xIndex], y[yIndex], z[zIndex], value);
  }

  private static HoleInterpolator createHoleInterpolator(List<InterpolationPoint> points) {
    double[] x1 = new double[points.size()];
    double[] y1 = new double[points.size()];
    double[] z1 = new double[points.size()];
    double[][][] v1 = new double[points.size()][points.size()][points.size()];

    for (int x1i = 0; x1i < x1.length; x1i++) {
      for (int y1i = 0; y1i < y1.length; y1i++) {
        for (int z1i = 0; z1i < z1.length; z1i++) {
          InterpolationPoint point = points.remove(0);
          x1[x1i] = point.getX();
          y1[y1i] = point.getY();
          z1[z1i] = point.getZ();
          v1[x1i][y1i][z1i] = point.getValue();
        }
      }
    }

    return new HoleInterpolator(x1, y1, z1, v1);
  }

  private void removeBubbles(List<InterpolationPoint> holes) {
    boolean replacedOneHole = false;
    do {

      List<Interpolation> interpolations = new ArrayList<>();

      for (InterpolationPoint hole : holes) {

        List<HoleInterpolator> interpolators = new ArrayList<>();

        InterpolationPoint negativeX = getPoint(hole.getxIndex() - 1, hole.getyIndex(), hole.getzIndex());
        InterpolationPoint positiveX = getPoint(hole.getxIndex() + 1, hole.getyIndex(), hole.getzIndex());
        InterpolationPoint negativeY = getPoint(hole.getxIndex(), hole.getyIndex() - 1, hole.getzIndex());
        InterpolationPoint positiveY = getPoint(hole.getxIndex(), hole.getyIndex() + 1, hole.getzIndex());
        InterpolationPoint negativeZ = getPoint(hole.getxIndex(), hole.getyIndex(), hole.getzIndex() - 1);
        InterpolationPoint positiveZ = getPoint(hole.getxIndex(), hole.getyIndex(), hole.getzIndex() + 1);

        if (negativeX != null &&
            positiveX != null &&
            negativeY != null &&
            positiveY != null &&
            negativeZ != null &&
            positiveZ != null) {

          List<InterpolationPoint> points = new ArrayList<>(6);
          points.add(negativeX);
          points.add(positiveX);
          points.add(negativeY);
          points.add(positiveY);
          points.add(negativeZ);
          points.add(positiveZ);
          Collections.sort(points);

          interpolators.add(createHoleInterpolator(points));
        }


        InterpolationPoint cornerPosXPosYPosZ = getPoint(hole.getxIndex() + 1, hole.getyIndex() + 1, hole.getzIndex() + 1);
        InterpolationPoint cornerPosXNegYPosZ = getPoint(hole.getxIndex() + 1, hole.getyIndex() - 1, hole.getzIndex() + 1);
        InterpolationPoint cornerPosXPosYNegZ = getPoint(hole.getxIndex() + 1, hole.getyIndex() + 1, hole.getzIndex() - 1);
        InterpolationPoint cornerPosXNegYNegZ = getPoint(hole.getxIndex() + 1, hole.getyIndex() - 1, hole.getzIndex() - 1);
        InterpolationPoint cornerNegXPosYPosZ = getPoint(hole.getxIndex() - 1, hole.getyIndex() + 1, hole.getzIndex() + 1);
        InterpolationPoint cornerNegXPosYNegZ = getPoint(hole.getxIndex() - 1, hole.getyIndex() + 1, hole.getzIndex() - 1);
        InterpolationPoint cornerNegXNegYPosZ = getPoint(hole.getxIndex() - 1, hole.getyIndex() - 1, hole.getzIndex() + 1);
        InterpolationPoint cornerNegXNegYNegZ = getPoint(hole.getxIndex() - 1, hole.getyIndex() - 1, hole.getzIndex() - 1);

        if (cornerPosXPosYPosZ != null &&
            cornerPosXNegYPosZ != null &&
            cornerPosXPosYNegZ != null &&
            cornerPosXNegYNegZ != null &&
            cornerNegXPosYPosZ != null &&
            cornerNegXPosYNegZ != null &&
            cornerNegXNegYPosZ != null &&
            cornerNegXNegYNegZ != null) {

          List<InterpolationPoint> points = new ArrayList<>(6);
          points.add(cornerPosXPosYPosZ);
          points.add(cornerPosXNegYPosZ);
          points.add(cornerPosXPosYNegZ);
          points.add(cornerPosXNegYNegZ);
          points.add(cornerNegXPosYPosZ);
          points.add(cornerNegXPosYNegZ);
          points.add(cornerNegXNegYPosZ);
          points.add(cornerNegXNegYNegZ);
          Collections.sort(points);

          interpolators.add(createHoleInterpolator(points));
        }

        InterpolationPoint edgePosXPosY = getPoint(hole.getxIndex() + 1, hole.getyIndex() + 1, hole.getzIndex());
        InterpolationPoint edgePosXNegY = getPoint(hole.getxIndex() + 1, hole.getyIndex() - 1, hole.getzIndex());
        InterpolationPoint edgeNegXPosY = getPoint(hole.getxIndex() - 1, hole.getyIndex() + 1, hole.getzIndex());
        InterpolationPoint edgeNegXNegY = getPoint(hole.getxIndex() - 1, hole.getyIndex() - 1, hole.getzIndex());

        InterpolationPoint edgePosYPosZ = getPoint(hole.getxIndex(), hole.getyIndex() + 1, hole.getzIndex() + 1);
        InterpolationPoint edgePosYNegZ = getPoint(hole.getxIndex(), hole.getyIndex() + 1, hole.getzIndex() - 1);
        InterpolationPoint edgeNegYPosZ = getPoint(hole.getxIndex(), hole.getyIndex() - 1, hole.getzIndex() + 1);
        InterpolationPoint edgeNegZNegZ = getPoint(hole.getxIndex(), hole.getyIndex() - 1, hole.getzIndex() - 1);

        InterpolationPoint edgePosXPosZ = getPoint(hole.getxIndex() + 1, hole.getyIndex(), hole.getzIndex() + 1);
        InterpolationPoint edgePosXNegZ = getPoint(hole.getxIndex() + 1, hole.getyIndex(), hole.getzIndex() - 1);
        InterpolationPoint edgeNegXPosZ = getPoint(hole.getxIndex() - 1, hole.getyIndex(), hole.getzIndex() + 1);
        InterpolationPoint edgeNegXNegZ = getPoint(hole.getxIndex() - 1, hole.getyIndex(), hole.getzIndex() - 1);

        if (edgePosXPosY != null &&
            edgePosXNegY != null &&
            edgeNegXPosY != null &&
            edgeNegXNegY != null &&
            edgePosYPosZ != null &&
            edgePosYNegZ != null &&
            edgeNegYPosZ != null &&
            edgeNegZNegZ != null &&
            edgePosXPosZ != null &&
            edgePosXNegZ != null &&
            edgeNegXPosZ != null &&
            edgeNegXNegZ != null) {

          List<InterpolationPoint> points = new ArrayList<>(6);
          points.add(edgePosXPosY);
          points.add(edgePosXNegY);
          points.add(edgeNegXPosY);
          points.add(edgeNegXNegY);
          points.add(edgePosYPosZ);
          points.add(edgePosYNegZ);
          points.add(edgeNegYPosZ);
          points.add(edgeNegZNegZ);
          points.add(edgePosXPosZ);
          points.add(edgePosXNegZ);
          points.add(edgeNegXPosZ);
          points.add(edgeNegXNegZ);
          Collections.sort(points);

          interpolators.add(createHoleInterpolator(points));
        }

        if (!interpolators.isEmpty()) {
          interpolations.add(new Interpolation(hole, interpolators));
        }
      }

      for (Interpolation interpolation : interpolations) {
        double[] values = new double[interpolation.getInterpolators().size()];
        int i = 0;
        for (HoleInterpolator interpolator : interpolation.getInterpolators()) {
          values[i++] = interpolator.interpolate(interpolation.getHole());
        }
        double interpolatedValue = DoubleStream.of(values).average().orElseThrow(() -> new IllegalStateException("Interpolated mean cannot be calculated"));
        holes.remove(interpolation.getHole());
        deNanValues[interpolation.getHole().getxIndex()][interpolation.getHole().getyIndex()][interpolation.getHole().getzIndex()] = interpolatedValue;
        replacedOneHole = true;
      }



    } while (replacedOneHole && !holes.isEmpty());
  }

  private void removeEdgeBubbles(List<InterpolationPoint> holes) {
    boolean replacedOneHole = false;
    do {

      List<Interpolation> interpolations = new ArrayList<>();

      for (InterpolationPoint hole : holes) {

        List<HoleInterpolator> interpolators = new ArrayList<>();

        InterpolationPoint negativeX = getPoint(hole.getxIndex() - 1, hole.getyIndex(), hole.getzIndex());
        InterpolationPoint positiveX = getPoint(hole.getxIndex() + 1, hole.getyIndex(), hole.getzIndex());
        InterpolationPoint negativeY = getPoint(hole.getxIndex(), hole.getyIndex() - 1, hole.getzIndex());
        InterpolationPoint positiveY = getPoint(hole.getxIndex(), hole.getyIndex() + 1, hole.getzIndex());
        InterpolationPoint negativeZ = getPoint(hole.getxIndex(), hole.getyIndex(), hole.getzIndex() - 1);
        InterpolationPoint positiveZ = getPoint(hole.getxIndex(), hole.getyIndex(), hole.getzIndex() + 1);

        if (negativeX != null &&
            positiveX != null &&
            negativeY != null &&
            positiveY != null &&
            negativeZ != null &&
            positiveZ != null) {

          List<InterpolationPoint> points = new ArrayList<>(6);
          points.add(negativeX);
          points.add(positiveX);
          points.add(negativeY);
          points.add(positiveY);
          points.add(negativeZ);
          points.add(positiveZ);
          Collections.sort(points);

          interpolators.add(createHoleInterpolator(points));
        }


        InterpolationPoint cornerPosXPosYPosZ = getPoint(hole.getxIndex() + 1, hole.getyIndex() + 1, hole.getzIndex() + 1);
        InterpolationPoint cornerPosXNegYPosZ = getPoint(hole.getxIndex() + 1, hole.getyIndex() - 1, hole.getzIndex() + 1);
        InterpolationPoint cornerPosXPosYNegZ = getPoint(hole.getxIndex() + 1, hole.getyIndex() + 1, hole.getzIndex() - 1);
        InterpolationPoint cornerPosXNegYNegZ = getPoint(hole.getxIndex() + 1, hole.getyIndex() - 1, hole.getzIndex() - 1);
        InterpolationPoint cornerNegXPosYPosZ = getPoint(hole.getxIndex() - 1, hole.getyIndex() + 1, hole.getzIndex() + 1);
        InterpolationPoint cornerNegXPosYNegZ = getPoint(hole.getxIndex() - 1, hole.getyIndex() + 1, hole.getzIndex() - 1);
        InterpolationPoint cornerNegXNegYPosZ = getPoint(hole.getxIndex() - 1, hole.getyIndex() - 1, hole.getzIndex() + 1);
        InterpolationPoint cornerNegXNegYNegZ = getPoint(hole.getxIndex() - 1, hole.getyIndex() - 1, hole.getzIndex() - 1);

        if (cornerPosXPosYPosZ != null &&
            cornerPosXNegYPosZ != null &&
            cornerPosXPosYNegZ != null &&
            cornerPosXNegYNegZ != null &&
            cornerNegXPosYPosZ != null &&
            cornerNegXPosYNegZ != null &&
            cornerNegXNegYPosZ != null &&
            cornerNegXNegYNegZ != null) {

          List<InterpolationPoint> points = new ArrayList<>(6);
          points.add(cornerPosXPosYPosZ);
          points.add(cornerPosXNegYPosZ);
          points.add(cornerPosXPosYNegZ);
          points.add(cornerPosXNegYNegZ);
          points.add(cornerNegXPosYPosZ);
          points.add(cornerNegXPosYNegZ);
          points.add(cornerNegXNegYPosZ);
          points.add(cornerNegXNegYNegZ);
          Collections.sort(points);

          interpolators.add(createHoleInterpolator(points));
        }

        InterpolationPoint edgePosXPosY = getPoint(hole.getxIndex() + 1, hole.getyIndex() + 1, hole.getzIndex());
        InterpolationPoint edgePosXNegY = getPoint(hole.getxIndex() + 1, hole.getyIndex() - 1, hole.getzIndex());
        InterpolationPoint edgeNegXPosY = getPoint(hole.getxIndex() - 1, hole.getyIndex() + 1, hole.getzIndex());
        InterpolationPoint edgeNegXNegY = getPoint(hole.getxIndex() - 1, hole.getyIndex() - 1, hole.getzIndex());

        InterpolationPoint edgePosYPosZ = getPoint(hole.getxIndex(), hole.getyIndex() + 1, hole.getzIndex() + 1);
        InterpolationPoint edgePosYNegZ = getPoint(hole.getxIndex(), hole.getyIndex() + 1, hole.getzIndex() - 1);
        InterpolationPoint edgeNegYPosZ = getPoint(hole.getxIndex(), hole.getyIndex() - 1, hole.getzIndex() + 1);
        InterpolationPoint edgeNegZNegZ = getPoint(hole.getxIndex(), hole.getyIndex() - 1, hole.getzIndex() - 1);

        InterpolationPoint edgePosXPosZ = getPoint(hole.getxIndex() + 1, hole.getyIndex(), hole.getzIndex() + 1);
        InterpolationPoint edgePosXNegZ = getPoint(hole.getxIndex() + 1, hole.getyIndex(), hole.getzIndex() - 1);
        InterpolationPoint edgeNegXPosZ = getPoint(hole.getxIndex() - 1, hole.getyIndex(), hole.getzIndex() + 1);
        InterpolationPoint edgeNegXNegZ = getPoint(hole.getxIndex() - 1, hole.getyIndex(), hole.getzIndex() - 1);

        if (edgePosXPosY != null &&
            edgePosXNegY != null &&
            edgeNegXPosY != null &&
            edgeNegXNegY != null &&
            edgePosYPosZ != null &&
            edgePosYNegZ != null &&
            edgeNegYPosZ != null &&
            edgeNegZNegZ != null &&
            edgePosXPosZ != null &&
            edgePosXNegZ != null &&
            edgeNegXPosZ != null &&
            edgeNegXNegZ != null) {

          List<InterpolationPoint> points = new ArrayList<>(6);
          points.add(edgePosXPosY);
          points.add(edgePosXNegY);
          points.add(edgeNegXPosY);
          points.add(edgeNegXNegY);
          points.add(edgePosYPosZ);
          points.add(edgePosYNegZ);
          points.add(edgeNegYPosZ);
          points.add(edgeNegZNegZ);
          points.add(edgePosXPosZ);
          points.add(edgePosXNegZ);
          points.add(edgeNegXPosZ);
          points.add(edgeNegXNegZ);
          Collections.sort(points);

          interpolators.add(createHoleInterpolator(points));
        }

        if (interpolators.size() > 0) {
          interpolations.add(new Interpolation(hole, interpolators));
        }
      }

      for (Interpolation interpolation : interpolations) {
        double[] values = new double[interpolation.getInterpolators().size()];
        int i = 0;
        for (HoleInterpolator interpolator : interpolation.getInterpolators()) {
          values[i++] = interpolator.interpolate(interpolation.getHole());
        }
        double interpolatedValue = DoubleStream.of(values).average().orElseThrow(() -> new IllegalStateException("Interpolated mean cannot be calculated"));
        holes.remove(interpolation.getHole());
        deNanValues[interpolation.getHole().getxIndex()][interpolation.getHole().getyIndex()][interpolation.getHole().getzIndex()] = interpolatedValue;
        replacedOneHole = true;
      }



    } while (replacedOneHole && !holes.isEmpty());
  }

}
