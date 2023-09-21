package edu.colorado.cires.wod.iquodqc.check.en.backgroundcheck;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.en.EnBackgroundChecker;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.en.EnBgCheckInfoParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.Row;

public class BackgroundCheck extends CommonCastCheck {

  private static EnBgCheckInfoParameters parameters;
  private static EnBackgroundChecker enBackgroundChecker;
  private Properties properties;

  @Override
  public String getName() {
    return CheckNames.EN_BACKGROUND_CHECK.getName();
  }

  @Override
  public Collection<String> dependsOn() {
    return Collections.singleton(CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName());
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (parameters == null) {
      loadParameters(properties);
    }
    return super.checkUdf(row);
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    throw new UnsupportedOperationException("This method is not used");
  }


  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    return enBackgroundChecker.getFailedDepths(cast, otherTestResults).getFailures();
  }

  private static void loadParameters(Properties properties) {
    synchronized (BackgroundCheck.class) {
      if (parameters == null) {
        parameters = EnBgCheckInfoParametersReader.loadParameters(properties);
        enBackgroundChecker = new EnBackgroundChecker(parameters);
      }
    }
  }

}
