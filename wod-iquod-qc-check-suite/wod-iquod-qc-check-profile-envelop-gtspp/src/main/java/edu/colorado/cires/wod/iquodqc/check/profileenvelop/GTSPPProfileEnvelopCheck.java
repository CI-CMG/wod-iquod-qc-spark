package edu.colorado.cires.wod.iquodqc.check.profileenvelop;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.check.profileenvelop.ProfileEnvelop.ProfileThresholds;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;
import java.util.List;

public class GTSPPProfileEnvelopCheck extends CommonCastCheck {
  private static final List<ProfileThresholds> THRESHOLDS = List.of(
      new ProfileThresholds(0, 25, -2, 37),
      new ProfileThresholds(25, 50, -2, 36),
      new ProfileThresholds(50, 100, -2, 36),
      new ProfileThresholds(100, 150, -2, 34),
      new ProfileThresholds(150, 200, -2, 33),
      new ProfileThresholds(200, 300, -2, 29),
      new ProfileThresholds(300, 400, -2, 27),
      new ProfileThresholds(400, 1100, -2, 27),
      new ProfileThresholds(1100, 3000, -1.5, 18),
      new ProfileThresholds(3000, 5500, -1.5, 7),
      new ProfileThresholds(5500, 12000, -1.5, 4)
  );

  @Override
  public String getName() {
    return "GTSPP_PROFILE_ENVELOP_CHECK";
  }

  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    return ProfileEnvelop.checkProfileEnvelop(
        cast.getDepths(), 
        THRESHOLDS
    );
  }

  

}
