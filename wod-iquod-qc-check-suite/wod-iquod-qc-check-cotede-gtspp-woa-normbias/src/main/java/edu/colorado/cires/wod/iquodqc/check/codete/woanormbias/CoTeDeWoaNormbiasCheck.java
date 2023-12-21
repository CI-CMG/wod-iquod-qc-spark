package edu.colorado.cires.wod.iquodqc.check.codete.woanormbias;


import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.Woa;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaGetter;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.spark.sql.Row;

public class CoTeDeWoaNormbiasCheck extends CommonCastCheck {

  private static final double THRESHOLD = 3D;
  private static final int MIN_SAMPLES = 3;

  private Properties properties;
  private static WoaGetter woaGetter;

  @Override
  public String getName() {
    return CheckNames.COTEDE_GTSPP_WOA_NORMBIAS.getName();
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
  protected Collection<Integer> getFailedDepths(Cast cast) {
    Set<Integer> failed = new LinkedHashSet<>();
    List<Depth> depths = cast.getDepths();
    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      final int index = i;
      DepthUtils.getTemperature(depth).ifPresent(pd -> {
        double temp = pd.getValue();
        Woa woa = woaGetter.getWoa(cast.getTimestamp(), depth.getDepth(), cast.getLongitude(), cast.getLatitude());
        woa.getMean().ifPresent(mean -> {
          woa.getStandardDeviation().ifPresent(stdDev -> {
            woa.getNumberOfObservations().ifPresent(nSamples -> {
              double woaBias = temp - mean;
              double woaNormBias = woaBias / stdDev;
              double woaNormBiasAbs = Math.abs(woaNormBias);
              if (nSamples >= MIN_SAMPLES && woaNormBiasAbs > THRESHOLD) {
                failed.add(index);
              }
            });
          });
        });
      });
    }
    return failed;
  }

  private static void loadParameters(Properties properties) {
    synchronized (CoTeDeWoaNormbiasCheck.class) {
      if (woaGetter == null) {
        woaGetter = new WoaGetter(WoaParametersReader.loadParameters(properties));
      }
    }
  }

}
