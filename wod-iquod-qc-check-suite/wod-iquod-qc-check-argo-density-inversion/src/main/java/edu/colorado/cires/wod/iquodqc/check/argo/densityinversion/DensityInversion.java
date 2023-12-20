package edu.colorado.cires.wod.iquodqc.check.argo.densityinversion;

import javax.annotation.Nullable;
import org.apache.commons.math3.util.FastMath;

public class DensityInversion {
  
  private static final double SSO = 35.16504;
  private static final double CP0 = 3991.86795711963;
  private static final double KELVIN = 273.15;
  private static final double SFAC = 0.0248826675584615;
  private static final int N0 = 0;
  private static final int N1 = 1;
  private static final int N2 = 2;
  
  public static InsituDensityComputation computeInsituDensity(double salinity, double temperature, double pressure, @Nullable InsituDensityComputation previousDensityComputation) {
    double potentialTemperature = computePotentialTemperature(salinity, temperature, pressure);
    double insituDensity = computeInsituDensity(salinity, potentialTemperature);
    
    if (previousDensityComputation == null) {
      return new InsituDensityComputation(
          insituDensity,
          pressure,
          Double.NaN
      );
    }
    return new InsituDensityComputation(
        insituDensity,
        pressure,
        FastMath.signum(pressure - previousDensityComputation.pressure) * (insituDensity - previousDensityComputation.density)
    );
  }
  
  public static class InsituDensityComputation {
    private final double density;
    private final double pressure;
    private final double difference;

    private InsituDensityComputation(double density, double pressure, double difference) {
      this.density = density;
      this.pressure = pressure;
      this.difference = difference;
    }

    public double getDifference() {
      return difference;
    }
  }
  
  private static double computeInsituDensity(double salinity, double temperature) {
      return 1. / computeGibbs(N0, N0, N1, salinity, temperature, 0.0);
  }
  
  private static double computePotentialTemperature(double salinity, double temperature, double pressure) {
    salinity = addFloorToSalinity(salinity);

    double pt = computePt(salinity, temperature, pressure);

    double dEntropyDT = computeDEntropyDT(salinity, pt);
    double trueEntropyPart = computeEntropyPart(salinity, temperature, pressure);

    double ptOld;
    for (int i = 0; i < 2; i++) {
      ptOld = pt;
      double dEntropy = computeEntropyPart(salinity, ptOld, 0., trueEntropyPart);
      pt = computeNewPt(ptOld, dEntropy, dEntropyDT);
      double ptm = computePtm(pt, ptOld);
      double dEntropyDt = -computeGibbs(N0, N2, N0, salinity, ptm, 0.);
      pt = computeNewPt(ptOld, dEntropy, dEntropyDt);
    }

    return pt;
  }
  
  public static double computeGibbs(int ns, int nt, int npr, double SA, double t, double p) {
    double gibbs;
    double g03;
    double g08;

    double x2 = SFAC * SA;
    double x = FastMath.sqrt(x2);
    double y = t * 0.025;
    double z = p * 1E-4;

    if ((ns == 0) & (nt == 2) & (npr == 0)) {
      g03 = (-24715.571866078 + z * (2910.0729080936 + z *
          (-1513.116771538718 + z * (546.959324647056 + z *
              (-111.1208127634436 + 8.68841343834394 * z)))) +
          y * (4420.4472249096725 + z * (-4035.04669887042 +
              z * (2996.162344914912 + z * (-1437.2719839264719 +
                  (292.8075111563232 - 9.978426372534301 * z) * z))) +
              y * (-1778.231237203896 + z * (4775.621344883664 +
                  z * (-3621.784567462512 + (1826.356460806092 -
                      316.49805267936244 * z) * z)) +
                  y * (1160.5182516851419 + z * (-3892.3662123519 +
                      z * (2410.4130980405 + z * (-1105.446104680304 +
                          129.6381336154442 * z))) +
                      y * (-569.531539542516 + y * (128.13429152494615 -
                          404.50541014508605 * z) +
                          z * (1905.341809925355 + z * (-668.691951421377 +
                              245.11816254543362 * z)))))));
      if (SA != 0 && !Double.isNaN(SA)) {
        g08 = x2 * (1760.062705994408 + x * (-86.1329351956084 +
            x * (-137.1145018408982 + y * (296.20061691375236 +
                y * (-205.67709290374563 + 49.9394019139016 * y))) +
            z * (766.116132004952 + z * (-108.3834525034224 +
                51.2796974779828 * z)) +
            y * (-60.136422517125 - 2761.9195908075417 * z +
                y * (10.50720794170734 + 2814.78225133626 * z))) +
            y * (-1351.605895580406 + y * (1097.1125373015109 +
                y * (-433.20648175062206 + 63.905091254154904 * y) +
                z * (-3572.7449038462437 + (896.713693665072 -
                    437.84750280190565 * z) * z)) +
                z * (4165.4688847996085 + z * (-1229.337851789418 +
                    (681.370187043564 - 66.7696405958478 * z) * z))) +
            z * (-1721.528607567954 + z * (674.819060538734 +
                z * (-356.629112415276 + (88.4080716616 -
                    15.84003094423364 * z) * z))));
      } else {
        g08 = 0;
      }
      gibbs = (g03 + g08) * 0.000625;
    } else if ((ns == 0) & (nt == 0) & (npr == 1)) {
      g03 = (100015.695367145 + z * (-5089.1530840726 +
          z * (853.5533353388611 + z * (-133.2587017014444 +
              (21.0131554401542 - 3.278571068826234 * z) * z))) +
          y * (-270.983805184062 + z * (1552.307223226202 +
              z * (-589.53765264366 + (115.91861051767 -
                  10.664504175916349 * z) * z)) +
              y * (1455.0364540468 + z * (-1513.116771538718 +
                  z * (820.438986970584 + z * (-222.2416255268872 +
                      21.72103359585985 * z))) +
                  y * (-672.50778314507 + z * (998.720781638304 +
                      z * (-718.6359919632359 + (195.2050074375488 -
                          8.31535531044525 * z) * z)) +
                      y * (397.968445406972 + z * (-603.630761243752 +
                          (456.589115201523 - 105.4993508931208 * z) * z) +
                          y * (-194.618310617595 + y * (63.5113936641785 -
                              9.63108119393062 * y +
                              z * (-44.5794634280918 + 24.511816254543362 * z)) +
                              z * (241.04130980405 + z * (-165.8169157020456 +
                                  25.92762672308884 * z))))))));
      if (SA != 0 && !Double.isNaN(SA)) {
        g08 = x2 * (-3310.49154044839 + z * (769.588305957198 +
            z * (-289.5972960322374 + (63.3632691067296 -
                13.1240078295496 * z) * z)) +
            x * (199.459603073901 + x * (-54.7919133532887 +
                36.0284195611086 * x - 22.6683558512829 * y +
                (-8.16387957824522 - 90.52653359134831 * z) * z) +
                z * (-104.588181856267 + (204.1334828179377 -
                    13.65007729765128 * z) * z) +
                y * (-175.292041186547 + (166.3847855603638 -
                    88.449193048287 * z) * z +
                    y * (383.058066002476 + y * (-460.319931801257 +
                        234.565187611355 * y) +
                        z * (-108.3834525034224 + 76.9195462169742 * z)))) +
            y * (729.116529735046 + z * (-687.913805923122 +
                z * (374.063013348744 + z * (-126.627857544292 +
                    35.23294016577245 * z))) +
                y * (-860.764303783977 + y * (694.244814133268 +
                    y * (-297.728741987187 + (149.452282277512 -
                        109.46187570047641 * z) * z) +
                    z * (-409.779283929806 + (340.685093521782 -
                        44.5130937305652 * z) * z)) +
                    z * (674.819060538734 + z * (-534.943668622914 +
                        (176.8161433232 - 39.600077360584095 * z) * z)))));
      } else {
        g08 = 0;
      }
      gibbs = (g03 + g08) * 1e-8;
    } else {
      throw new IllegalStateException("Illegal derivative of the Gibbs function");
    }

    return gibbs;
  }
  
  private static double computePtm(double pt, double ptOld) {
    return 0.5 * (pt + ptOld);
  }

  private static double computeNewPt(double ptOld, double dEntropy, double dEntropyDT) {
    return ptOld - (dEntropy / dEntropyDT);
  }
  
  private static double computeEntropyPart(double salinity, double temperature, double pressure, double trueEntropyPart) {
    return computeEntropyPart(salinity, temperature, pressure) - trueEntropyPart;
  }
  
  private static double computeEntropyPart(double salinity, double temperature, double pressure) {
    double x = FastMath.sqrt(
        salinity * SFAC
    );
    double y = temperature * 0.025;
    double z = pressure * 1E-4;

    return computeEntropy(x, y, z);
  }
  
  private static double computeEntropy(double x, double y, double z) {
    double g03 = (z * (-270.983805184062 + z * (776.153611613101 + z *
        (-196.51255088122 + (28.9796526294175 - 2.13290083518327 * z) *
            z))) + y * (-24715.571866078 + z * (2910.0729080936 + z *
        (-1513.116771538718 + z * (546.959324647056 + z *
            (-111.1208127634436 + 8.68841343834394 * z)))) + y *
        (2210.2236124548363 + z * (-2017.52334943521 + z *
            (1498.081172457456 + z * (-718.6359919632359 +
                (146.4037555781616 - 4.9892131862671505 * z) * z))) + y *
            (-592.743745734632 + z * (1591.873781627888 + z *
                (-1207.261522487504 + (608.785486935364 - 105.4993508931208 * z) *
                    z)) + y * (290.12956292128547 + z * (-973.091553087975 + z *
                (602.603274510125 + z * (-276.361526170076 + 32.40953340386105 *
                    z))) + y * (-113.90630790850321 + y * (21.35571525415769 -
                67.41756835751434 * z) + z * (381.06836198507096 + z *
                (-133.7383902842754 + 49.023632509086724 * z))))))));

    double g08 = (Math.pow(x, 2) * (z * (729.116529735046 + z * (-343.956902961561 + z *
        (124.687671116248 + z * (-31.656964386073 + 7.04658803315449 *
            z)))) + x * (x * (y * (-137.1145018408982 + y *
        (148.10030845687618 + y * (-68.5590309679152 + 12.4848504784754 *
            y))) - 22.6683558512829 * z) + z * (-175.292041186547 +
        (83.1923927801819 - 29.483064349429 * z) * z) + y *
        (-86.1329351956084 + z * (766.116132004952 + z *
            (-108.3834525034224 + 51.2796974779828 * z)) + y *
            (-30.0682112585625 - 1380.9597954037708 * z + y *
                (3.50240264723578 + 938.26075044542 * z)))) + y *
        (1760.062705994408 + y * (-675.802947790203 + y *
            (365.7041791005036 + y * (-108.30162043765552 + 12.78101825083098 *
                y) + z * (-1190.914967948748 + (298.904564555024 -
                145.9491676006352 * z) * z)) + z * (2082.7344423998043 + z *
            (-614.668925894709 + (340.685093521782 - 33.3848202979239 * z) *
                z))) + z * (-1721.528607567954 + z * (674.819060538734 + z *
            (-356.629112415276 + (88.4080716616 - 15.84003094423364 * z) *
                z))))));

    return -(g03 + g08) * 0.025;
  }
  
  private static double addFloorToSalinity(double salinity) {
    return FastMath.max(salinity, 0);
  }
  
  private static double computePt(double salinity, double temperature, double pressure) {
    double s1 = salinity * 35. / SSO;
    return (temperature + pressure * (8.65483913395442e-6 -
        s1 * 1.41636299744881e-6 -
        pressure * 7.38286467135737e-9 +
        temperature * (-8.38241357039698e-6 +
            s1 * 2.83933368585534e-8 +
            temperature * 1.77803965218656e-8 +
            pressure * 1.71155619208233e-10)));
  }
  
  private static double computeDEntropyDT(double salinity, double pt) {
    return CP0 / ((KELVIN + pt) * (1 - 0.05 * (1 - salinity / SSO)));
  }

}
