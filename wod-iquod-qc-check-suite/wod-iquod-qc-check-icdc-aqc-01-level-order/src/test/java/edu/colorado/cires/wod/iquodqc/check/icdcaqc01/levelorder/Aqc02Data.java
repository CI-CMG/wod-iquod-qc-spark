package edu.colorado.cires.wod.iquodqc.check.icdcaqc01.levelorder;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;

import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Aqc02Data {
  String header;
  List<Integer> qc = new ArrayList<>();
  List<Double> depth = new ArrayList<>();
  List<Double> temp = new ArrayList<>();
  List<Double> reorderedDepth = new ArrayList<>();
  List<Double> reorderedTemp = new ArrayList<>();

  private List<Depth> castDepths = new ArrayList<>();

  public Aqc02Data(String line) {
    String[] fields = line.trim().split("\\s+");
    this.header = fields[0];
  }

  public void addrow(String line) {
    String[] fields = line.trim().split("\\s+");
    double depth = Double.parseDouble(fields[1].substring(0, fields[1].length() - 1));
    double temp = Double.parseDouble(fields[2].substring(0, fields[2].length() - 1));
    if (depth < 0D){
      this.qc.add(this.depth.size());
    }
    addCastDepth(depth, temp);
    this.depth.add(depth);
    this.temp.add(temp);

    double depthR = Double.parseDouble(fields[3].substring(0, fields[3].length() - 1));
    if (depthR >= 0D) {
      this.reorderedDepth.add(depthR);
      String tempR = fields[4].substring(0, fields[4].length() - 2);
      int endChar = 2;
      if (fields[4].substring( fields[4].length() - 1).equals(")")){
        endChar = 3;
      }
      this.reorderedTemp.add(Double.parseDouble(fields[4].substring(0, fields[4].length() - endChar)));
    }
  }

  private void addCastDepth(double depth, double temp){
    castDepths.add(Depth.builder().withDepth(depth)
        .withData(Collections.singletonList(ProfileData.builder()
            .withVariableCode(TEMPERATURE).withValue(temp).build())).build());
  }
  public Cast buildCast() {
    return Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(8888)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 6)
        .withTime(12D)
        .withDepths(this.castDepths)
        .build();
  }

  public String getHeader() {
    return header;
  }

  public List<Integer> getQc() {
    return qc;
  }

  public List<Double> getDepth() {
    return depth;
  }

  public List<Double> getTemp() {
    return temp;
  }

  public List<Double> getReorderedDepth() {
    return reorderedDepth;
  }

  public List<Double> getReorderedTemp() {
    return reorderedTemp;
  }

}
