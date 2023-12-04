package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.*;

import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BuddyCheckFunctionsTest {

  @Test
  public void testDeterminePge() {
    List<Double> obev = Arrays.asList(
        0.6084686, 0.6404497, 0.71714824, 0.80820745, 0.8733429, 0.9246968, 0.96506375, 0.99397093, 0.9521459, 0.90908635, 0.86621714, 0.8212779,
        0.7749001, 0.72448194, 0.67216897, 0.61146814, 0.54763424, 0.4916569, 0.42034614, 0.35154426, 0.319869, 0.2792595, 0.23177402, 0.17920469,
        0.13677177, 0.09746764, 0.063787445, 0.040508095, 0.024542019, 0.0149869295, 0.009492636, 0.008447383, 0.0077661346, 0.0072632097,
        0.0067941556, 0.006324135, 0.0058511184, 0.00554978, 0.0054058074, 0.005261523, 0.0051170215, 0.0049723685
    );

    List<Double> bgev = Arrays.asList(
        9.545716,
        8.119028,
        6.385097,
        4.7410398,
        3.4449716,
        2.5705447,
        2.0354106,
        1.5532228,
        1.4210919,
        1.2850614,
        1.2102679,
        1.1434017,
        1.0814964,
        1.0166788,
        0.94669557,
        0.8653665,
        0.7751268,
        0.6823798,
        0.5869994,
        0.5047928,
        0.44898346,
        0.3774326,
        0.2966053,
        0.21216902,
        0.18206935,
        0.18333276,
        0.14735256,
        0.08491475,
        0.03991173,
        0.021484364,
        0.011088027,
        0.00948348,
        0.008399859,
        0.0075708115,
        0.007086405,
        0.006626666,
        0.0061639966,
        0.005852053,
        0.005678314,
        0.005504199,
        0.0053298213,
        0.005155261);

    double latitude = 55.6;
    int probeType = 7;
    double level = 1000D;
    assertEquals(1.0, BuddyCheckFunctions.determinePge(0, level, bgev, obev, latitude, probeType), 0.000001);
    assertEquals(1.0, BuddyCheckFunctions.determinePge(1, level, bgev, obev, latitude, probeType), 0.000001);
    assertEquals(1.0, BuddyCheckFunctions.determinePge(2, level, bgev, obev, latitude, probeType), 0.000001);
    assertEquals(1.0, BuddyCheckFunctions.determinePge(3, level, bgev, obev, latitude, probeType), 0.000001);
  }

  @Test
  public void testBuddyCovarianceTime() {
    /*
            p1 = util.testingProfile.fakeProfile([0,0,0],[0,0,0], date=[1900, 1, 1, 12])
        p2 = util.testingProfile.fakeProfile([0,0,0],[0,0,0], date=[1900, 1, 6, 12])
        buddyCovariance_5days = qctests.EN_std_lev_bkg_and_buddy_check.buddyCovariance(100, p1, p2, 1, 1, 1, 1)

        p1 = util.testingProfile.fakeProfile([0,0,0],[0,0,0], date=[1900, 1, 1, 12])
        p2 = util.testingProfile.fakeProfile([0,0,0],[0,0,0], date=[1900, 1, 11, 12])
        buddyCovariance_10days = qctests.EN_std_lev_bkg_and_buddy_check.buddyCovariance(100, p1, p2, 1, 1, 1, 1)

        assert buddyCovariance_5days * numpy.exp(-3) - buddyCovariance_10days < 1e-12, 'incorrect timescale behavior'
     */

    Cast p1 = Cast.builder()
        .withCastNumber(1)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 6)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build()
        ))
        .build();

    Cast p2 = Cast.builder()
        .withCastNumber(1)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 11)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build()
        ))
        .build();

    Cast p3 = Cast.builder()
        .withCastNumber(1)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 1)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build()
        ))
        .build();

    double buddyCovariance5days = BuddyCheckFunctions.buddyCovariance(100D, p1, p2, 1D, 1D, 1D, 1D);
    double buddyCovariance10days = BuddyCheckFunctions.buddyCovariance(100D, p1, p3, 1D, 1D, 1D, 1D);
    assertTrue(buddyCovariance5days * Math.exp(-3D) - buddyCovariance10days < 1E-12);
  }

  @Test
  public void testBuddyCovarianceMesoScale() {

    Cast p1 = Cast.builder()
        .withCastNumber(1)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 1)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build()
        ))
        .build();

    Cast p2 = Cast.builder()
        .withCastNumber(1)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 6)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build()
        ))
        .build();


    double buddyCovariance100km = BuddyCheckFunctions.buddyCovariance(100000D, p1, p2, 1D, 1D, 0D, 0D);
    double buddyCovariance200km = BuddyCheckFunctions.buddyCovariance(200000D, p1, p2, 1D, 1D, 0D, 0D);
    assertTrue(buddyCovariance100km * Math.exp(-1D) - buddyCovariance200km < 1E-12);
  }

  @Test
  public void testBuddyCovarianceSynopticScale() {

    Cast p1 = Cast.builder()
        .withCastNumber(1)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 1)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build()
        ))
        .build();

    Cast p2 = Cast.builder()
        .withCastNumber(1)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 6)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build(),
            Depth.builder().withDepth(0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(0).build())).build()
        ))
        .build();


    double buddyCovariance100km = BuddyCheckFunctions.buddyCovariance(100000D, p1, p2, 0D, 0D, 1D, 1D);
    double buddyCovariance500km = BuddyCheckFunctions.buddyCovariance(500000D, p1, p2, 0D, 0D, 1D, 1D);
    assertTrue(buddyCovariance100km * Math.exp(-1D) - buddyCovariance500km < 1E-12);
  }

}