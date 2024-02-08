package edu.colorado.cires.wod.iquodqc.check.codete.woanormbias;


import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getDepths;
import static edu.colorado.cires.wod.iquodqc.common.CastUtils.getTemperatures;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.BaseCoTeDeWoaNormbiasCheck;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.CoTeDeWoaNormbias;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.CoTeDeWoaNormbias.WoaNormbias;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParametersReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Row;
import ucar.ma2.InvalidRangeException;

public class CoTeDeGtsppWoaNormbiasCheck extends CommonCastCheck {

  private static WoaParameters woaParameters;
  private static final double THRESHOLD = 3D;
  private static final int MIN_SAMPLES = 3;
  private Properties properties;

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
    if (woaParameters == null) {
      loadParameters(properties);
    }
    return super.checkUdf(row);
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    List<WoaNormbias> normBiases;
    try {
      normBiases = CoTeDeWoaNormbias.computeNormbias(
          cast.getLatitude(),
          cast.getLongitude(),
          getTemperatures(cast),
          getDepths(cast),
          cast.getTimestamp(),
          woaParameters
      );
    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException(e);
    }

    List<Integer> failedDepths = new ArrayList<>(0);
    for (int i = 0; i < normBiases.size(); i++) {
      WoaNormbias normBias = normBiases.get(i);
      if (normBias.getNSamples() >= MIN_SAMPLES) {
        if (Math.abs(normBias.getValue()) > THRESHOLD) {
          failedDepths.add(i);
        }
      }
    }

    return failedDepths;
  }

  private static void loadParameters(Properties properties) {
    synchronized (BaseCoTeDeWoaNormbiasCheck.class) {
      if (woaParameters == null) {
        woaParameters = WoaParametersReader.loadParameters(properties);
      }
    }
  }
}
