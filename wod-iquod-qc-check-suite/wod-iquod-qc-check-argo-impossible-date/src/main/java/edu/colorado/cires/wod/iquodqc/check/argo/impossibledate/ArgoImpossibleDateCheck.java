package edu.colorado.cires.wod.iquodqc.check.argo.impossibledate;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ArgoImpossibleDateCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return "Argo_impossible_date_test";
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<Depth> depths = cast.getDepths();
    Set<Integer> failures = new LinkedHashSet<>();

    if (cast.getYear() < 1700 || isInvalidDate(cast)) {
      for (int i = 0; i < depths.size(); i++) {
        failures.add(i);
      }
    }

    return failures;
  }


  private static class HourMin {

    private final int hour;
    private final int min;


    private HourMin(int hour, int min) {
      this.hour = hour;
      this.min = min;
    }

    public int getHour() {
      return hour;
    }

    public int getMin() {
      return min;
    }
  }

  private static HourMin getTime(Cast cast) {
    double hoursWithFractionalHours = cast.getTime();
    int wholeHours = (int) hoursWithFractionalHours;

    double fractionalHours = hoursWithFractionalHours - (double) wholeHours;
    int minutes = (int) (60D * fractionalHours);

    return new HourMin(wholeHours, minutes);
  }

  private boolean isInvalidDate(Cast cast) {
    try {

      //TODO can this be null?
//      if (cast.getTime() != null) {
        HourMin hourMin = getTime(cast);
        LocalDateTime.of(
            cast.getYear(),
            cast.getMonth(),
            cast.getDay(),
            hourMin.getHour(),
            hourMin.getMin()
        );
//      } else {
//        LocalDate.of(
//            cast.getYear(),
//            cast.getMonth(),
//            cast.getDay() == null ? 1 : cast.getDay()
//        );
//      }
    } catch (DateTimeException e) {
      return true;
    }

    return false;
  }

}
