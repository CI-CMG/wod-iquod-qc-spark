package edu.colorado.cires.wod.iquodqc.common;

import static edu.colorado.cires.wod.iquodqc.common.DoubleUtils.lessThanOrEqual;

import edu.colorado.cires.mgg.teosgsw.TeosGsw10;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ObsUtils {

  public static double t48tot68(double t48){
    /*
    Convert from IPTS-48 to IPTS-68 temperature scales,
       as specified in the CF Standard Name information for
       sea_water_temperature
       http://cfconventions.org/Data/cf-standard-names/27/build/cf-standard-name-table.html

       temperatures are in degrees C
     */
      return t48 - 4.4e-6 * t48 * (100 - t48);
  }

  public static double t68tot90(double t68){
    /*
    Convert from IPTS-68 to ITS-90 temperature scales,
       as specified in the CF Standard Name information for
       sea_water_temperature
       http://cfconventions.org/Data/cf-standard-names/27/build/cf-standard-name-table.html

       temperatures are in degrees C
     */

    return 0.99976 * t68;
  }

  public static double depthToPressure(double z, double lat){
  /*   Wrapper function to convert from ocean depth to pressure.
       https://www.teos-10.org/pubs/gsw/pdf/z_from_p.pdf
       double geo_strf_dyn_height = 0.0
       double sea_surface_geopotential = 0.0
       This is similar in the python GSW when calling with two arguments. The height and geopotential are not use in the
       conversion.
 */
    return TeosGsw10.INSTANCE.gsw_p_from_z(-z,lat, 0.0, 0.0);
  }
  public static double pressureToDepth(double p, double lat){
  /*   Wrapper function to convert from ocean pressure to depth.
       https://www.teos-10.org/pubs/gsw/pdf/z_from_p.pdf
       double geo_strf_dyn_height = 0.0
       double sea_surface_geopotential = 0.0
       This is similar in the python GSW when calling with two arguments. The height and geopotential are not use in the
       conversion.
 */
    return -TeosGsw10.INSTANCE.gsw_z_from_p(p,lat, 0.0, 0.0);
  }

  public static double pottem(double t, double s, double dStart){
    return pottem(t, s, dStart, 0.0, false, 0.0);
  }
  public static double pottem(double t, double s, double dStart, double dEnd){
    return pottem(t, s, dStart, dEnd, false, 0.0);
  }

  public static double pottem(double t, double s, double dStart, double dEnd, boolean pressure){
    return pottem(t, s, dStart, dEnd, pressure, 0.0);
  }

  public static double pottem (double T, double s, double dStart, double dEnd, boolean pressure, double lat ){
//    Calculate the temperature of water if it is moved from
//    depth dStart to dEnd.
//
//    T: initial temperature of the water.
//    s: salinity of the water.
//    dStart: depth that the parcel of water starts at.
//    dEnd: depth that the parcel of water ends up at.
//        pressure: set to true if dStart and dEnd are pressures rather than depths.
//    lat: if pressure if False, latitude should also be specified.
    double P0 = dStart;
    double P1 = dEnd;
    if (!pressure){
      P0 = depthToPressure(dStart, lat);
      P1 = depthToPressure(dEnd, lat);
    }

    double DP = -1.0D;
    if (lessThanOrEqual(P0,P1)){
      DP = -DP;
    }
    double P = P0;
    double DS = s-35D;

    double TB = (T-((((-2.1687e-16*T+1.8676e-14)*T-4.6206e-13)*P0
        + ((2.7759e-12*T-1.1351e-10)*DS+((-5.4481e-14*T
        + 8.733e-12)*T-6.7795e-10)*T+1.8741e-8))*P0
        + (-4.2393e-8*T+1.8932e-6)*DS
        + ((6.6228e-10*T-6.836e-8)*T+8.5258e-6)*T+3.5803e-5)*DP);

    double test = 1;
    double TA = TB;
    while (!lessThanOrEqual(0,test)){
      TA = (TB+2.0e0*((((-2.1687e-16*T+1.8676e-14)*T-4.6206e-13)*P
          + ((2.7759e-12*T-1.1351e-10)*DS+((-5.4481e-14*T
          + 8.733e-12)*T-6.7795e-10)*T+1.8741e-8))*P
          + (-4.2393e-8*T+1.8932e-6)*DS
          + ((6.6228e-10*T-6.836e-8)*T+8.5258e-6)*T+3.5803e-5)*DP);

      P = P + DP;
      TB = T;
      T = TA;
      test = (P-P1)*(P-DP-P1);
    }
    return ((P1-P+DP)*T+(P-P1)*TB)/DP;
  }

  public static double density(double t, double s, double l, Optional<Double> latitude){
    /*
    Calculate the density/densities based on:
    t - potential temperature(s) in degC.
    s - salinity(s) in PSU.
    l - level(s) (either pressure or density) in m or db.
    latitude - only set if l contains depths (can be array or scalar) in deg.
    Code is ported from Ops_OceanRhoEOS25 from NEMOQC,
        which uses a 25 term expression given by McDougall et al (2003; JAOT 20, #5),
    which provides an accurate fit to the Feistel and Hagen (1995) equation of state.
        That code is in turn based on the UM routine in RHO_EOS25 (constants in STATE),
        but with salinity in PSU and density in kg m**-3, as in McDougall.

    Test values from McDougall et al (2005) are:
    t = 25C, s = 35psu, p = 2000 db => rho = 1031.654229 kg/m^2.
    20       20         1000             1017.726743
    12       40         8000             1062.928258

    This function is not properly tested for anything other than basic usage.
    */

//    VALUES NEEDED IN THE CALCULATION.
//    Small constant.
    double epsln = 1e-20;

//    25 coefficients in the realistic equation of state
    List<Double> a = Arrays.asList(9.99843699e+02,7.35212840e+00,-5.45928211e-02,3.98476704e-04,2.96938239e+00,
        -7.23268813e-03,2.12382341e-03,1.04004591e-02,1.03970529e-07, 5.18761880e-06,-3.24041825e-08,-1.23869360e-11);

    List<Double> b = Arrays.asList(1.00000000e+00,7.28606739e-03,-4.60835542e-05, 3.68390573e-07, 1.80809186e-10,
        2.14691708e-03,-9.27062484e-06,-1.78343643e-10, 4.76534122e-06,  1.63410736e-09,  5.30848875e-06,
        -3.03175128e-16,-1.27934137e-17);

    double p = l;
    if (latitude.isPresent()){
      p = depthToPressure(l, latitude.get());
    }

//     DENSITY CALCULATION.
    double p1   = p;
    double t1   = t;
    double s1   = s;
    double t2   = t1 * t1;
    double sp5  =Math.sqrt(s1);
    double p1t1 = p1 * t1;

    double num = (a.get(0) + t1*(a.get(1) + t1*(a.get(2)+a.get(3)*t1) )
        + s1*(a.get(4) + a.get(5)*t1  + a.get(6)*s1)
        + p1*(a.get(7) + a.get(8)*t2 + a.get(9)*s1 + p1*(a.get(10)+a.get(11)*t2)));

    double den = (b.get(0) + t1*(b.get(1) + t1*(b.get(2) + t1*(b.get(3) + t1*b.get(4))))
        + s1*(b.get(5) + t1*(b.get(6) + b.get(7)*t2) + sp5*(b.get(8) + b.get(9)*t2))
        + p1*(b.get(10) + p1t1*(b.get(11)*t2 + b.get(12)*p1)));

    double denr = 1.0/(epsln+den);

    return num * denr;
  }
}
