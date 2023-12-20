package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

public interface EtopoDataAccessor {

  double[] getLongitudes();

  double[] getLatitudes();

  float[][] getDepths(int minIndexLat, int maxIndexLat, int minIndexLon, int maxIndexLon);

}
