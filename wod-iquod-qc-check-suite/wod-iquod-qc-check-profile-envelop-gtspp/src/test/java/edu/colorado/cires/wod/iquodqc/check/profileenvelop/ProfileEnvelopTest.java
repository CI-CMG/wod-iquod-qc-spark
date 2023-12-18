package edu.colorado.cires.wod.iquodqc.check.profileenvelop;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.profileenvelop.ProfileEnvelop.ProfileThresholds;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ProfileEnvelopTest {
  
  @Test void testNoProfile() {
    List<ProfileThresholds> profiles = List.of(
        new ProfileThresholds(0, 10, 1, 100),
        new ProfileThresholds(20, 30, 100, 200)
    );
    
    List<Depth> depths = List.of(
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(35)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(10)
                        .build()
                )
            )
            .build()
    );
    
    assertEquals(0, ProfileEnvelop.checkProfileEnvelop(depths, profiles).size());
  }
  
  @Test void testNaNPressure() {
    List<Depth> depths = List.of(
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(Double.NaN)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(10)
                        .build()
                )
            )
            .build()
    );
    
    assertEquals(List.of(0), ProfileEnvelop.checkProfileEnvelop(depths, Collections.emptyList()));
  }
  
  @Test void testNanTemperature() {
    List<Depth> depths = List.of(
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(10)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(Double.NaN)
                        .build()
                )
            )
            .build()
    );
    
    assertEquals(List.of(0), ProfileEnvelop.checkProfileEnvelop(depths, Collections.emptyList()));
  }
  
  @Test void testWithinProfileFailure() {
    List<ProfileThresholds> profiles = List.of(
        new ProfileThresholds(0, 10, 1, 100),
        new ProfileThresholds(11, 20, 100, 200),
        new ProfileThresholds(21, 30, 200, 300)
    );
    
    List<Depth> depths = List.of(
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(5)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(50)
                        .build()
                )
            )
            .build(),
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(15)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(300)
                        .build()
                )
            )
            .build(),
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(25)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(250)
                        .build()
                )
            )
            .build()
    );
    
    assertEquals(List.of(1), ProfileEnvelop.checkProfileEnvelop(depths, profiles));
  }
  
  @Test void testWithinProfilePass() {
    List<ProfileThresholds> profiles = List.of(
        new ProfileThresholds(0, 10, 1, 100),
        new ProfileThresholds(11, 20, 100, 200),
        new ProfileThresholds(21, 30, 200, 300)
    );

    List<Depth> depths = List.of(
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(5)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(50)
                        .build()
                )
            )
            .build(),
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(15)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(150)
                        .build()
                )
            )
            .build(),
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(25)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(250)
                        .build()
                )
            )
            .build()
    );

    assertEquals(Collections.emptyList(), ProfileEnvelop.checkProfileEnvelop(depths, profiles));
  }

  @Test void testWithinProfilePassNearThreshold() {
    List<ProfileThresholds> profiles = List.of(
        new ProfileThresholds(0, 10, 1, 100),
        new ProfileThresholds(11, 20, 100, 200),
        new ProfileThresholds(21, 30, 200, 300)
    );

    List<Depth> depths = List.of(
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(9.999999)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(1.00000001)
                        .build()
                )
            )
            .build(),
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(11.00001)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(199.99999)
                        .build()
                )
            )
            .build(),
        Depth.builder()
            .withData(
                List.of(
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(29.99999)
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(299.99999)
                        .build()
                )
            )
            .build()
    );

    assertEquals(Collections.emptyList(), ProfileEnvelop.checkProfileEnvelop(depths, profiles));
  }

}
