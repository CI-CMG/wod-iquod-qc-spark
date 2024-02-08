package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getDepths;
import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getTemperatures;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.SignalProducingCastCheck;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParameters;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParametersReader;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.Row;
import ucar.ma2.InvalidRangeException;

public class CoTeDeCarsNormbiasCheck extends SignalProducingCastCheck {

  private Properties properties;
  private static CarsParameters carsParameters;

  @Override
  public String getName() {
    return CheckNames.COTEDE_CARS_NORMBIAS_CHECK.getName();
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (carsParameters == null) {
      loadParameters(properties);
    }
    return super.checkUdf(row);
  }

  /*
  Not intended to contribute to IQUoD flags. This check should only be generating a signal for use in further QC tests 
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    try {
      signal = CoTeDeCarsNormbias.computeNormbias(
          cast.getLatitude(),
          cast.getLongitude(),
          getTemperatures(cast),
          getDepths(cast),
          carsParameters
      );
    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException(e);
    }
    return Collections.emptyList();
  }

  private static void loadParameters(Properties properties) {
    synchronized (CoTeDeCarsNormbiasCheck.class) {
      if (carsParameters == null) {
        carsParameters = CarsParametersReader.loadParameters(properties);
      }
    }
  }
}
