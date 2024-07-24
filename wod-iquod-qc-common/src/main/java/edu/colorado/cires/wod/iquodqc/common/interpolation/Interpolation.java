package edu.colorado.cires.wod.iquodqc.common.interpolation;

import java.util.List;
import org.jetbrains.annotations.NotNull;

class Interpolation implements Comparable<Interpolation> {

  private final InterpolationPoint hole;
  private final List<HoleInterpolator> interpolators;

  public Interpolation(InterpolationPoint hole, List<HoleInterpolator> interpolators) {
    this.hole = hole;
    this.interpolators = interpolators;
  }

  public InterpolationPoint getHole() {
    return hole;
  }

  public List<HoleInterpolator> getInterpolators() {
    return interpolators;
  }

  @Override
  public int compareTo(@NotNull Interpolation o) {
    if (interpolators.size() == o.interpolators.size()) {
      int points = 0;
      for (HoleInterpolator interpolator : interpolators) {
        points = points + interpolator.getX().length;
      }
      int oPoints = 0;
      for (HoleInterpolator interpolator : o.interpolators) {
        oPoints = oPoints + interpolator.getX().length;
      }
      if (points == oPoints) {
        return hole.compareTo(o.hole);
      }
      return Integer.compare(oPoints, points);
    }
    return Integer.compare(o.interpolators.size(), interpolators.size());
  }
}
