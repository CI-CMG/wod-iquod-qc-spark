package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getDepths;
import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getTemperatures;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.SignalProducingCastCheck;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsGetter;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParametersReader;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;

public class CoTeDeCarsNormbiasCheck extends SignalProducingCastCheck {

  private Properties properties;
  private static CarsGetter carsGetter;

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
    if (carsGetter == null) {
      loadParameters(properties);
    }
    return super.checkUdf(row);
  }

  @Override
  protected List<Double> produceSignal(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    return Arrays.stream(CoTeDeCarsNormbias.computeCarsNormbiases(
        getTemperatures(cast),
        getDepths(cast),
        cast.getLatitude(),
        cast.getLongitude(),
        carsGetter
    )).boxed().collect(Collectors.toList());
  }

  /*
  Not intended to contribute to IQUoD flags. This check should only be generating a signal for use in further QC tests 
   */
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, List<Double> signal, Map<String, CastCheckResult> otherTestResults) {
    return Collections.emptyList();
  }

  private static void loadParameters(Properties properties) {
    synchronized (CoTeDeCarsNormbiasCheck.class) {
      if (carsGetter == null) {
        carsGetter = new CarsGetter(CarsParametersReader.loadParameters(properties));
      }
    }
  }
}
