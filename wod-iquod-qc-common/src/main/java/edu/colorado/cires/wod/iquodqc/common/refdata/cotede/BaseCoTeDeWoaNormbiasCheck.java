package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;


import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.SignalProducingCastCheck;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.Row;

public abstract class BaseCoTeDeWoaNormbiasCheck extends SignalProducingCastCheck {

  private final double threshold;
  private final int MIN_SAMPLES = 3;
  private Properties properties;
  private static WoaGetter woaGetter;

  protected BaseCoTeDeWoaNormbiasCheck(double threshold){
    this.threshold = threshold;
  }

  @Override
  public void initialize(CastCheckInitializationContext initContext) {
    properties = initContext.getProperties();
  }

  @Override
  protected Row checkUdf(Row row) {
    if (woaGetter == null) {
      loadParameters(properties);
    }
    return super.checkUdf(row);
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast, Map<String, CastCheckResult> otherTestResults) {
    List<Depth> depths = cast.getDepths();
    List<Integer> failed = new ArrayList<>(0);
    signal = new ArrayList<>(0);
    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      int index = i;
      DepthUtils.getTemperature(depth).ifPresent(pd -> {
        double temp = pd.getValue();
        WoaStats stats = woaGetter.getStats(cast.getTimestamp(), depth.getDepth(), cast.getLongitude(), cast.getLatitude());
        stats.getMean().ifPresent(mean -> {
          stats.getStandardDeviation().ifPresent(stdDev -> {
            stats.getNumberOfObservations().ifPresent(nSamples -> {
              double woaBias = temp - mean;
              double woaNormBias = woaBias / stdDev;
              signal.add(woaNormBias);
              if (nSamples >= MIN_SAMPLES && woaNormBias > threshold) {
                failed.add(index);
              }
            });
          });
        });
      });
      if (signal.size() == i) {
        signal.add(Double.NaN);
      }
    }
    return failed;
  }

  private static void loadParameters(Properties properties) {
    synchronized (BaseCoTeDeWoaNormbiasCheck.class) {
      if (woaGetter == null) {
        woaGetter = new WoaGetter(WoaParametersReader.loadParameters(properties));
      }
    }
  }

}
