package edu.colorado.cires.wod.iquodqc.check.icdcaqc06.ntempextrema;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;

import com.google.common.collect.ImmutableMap;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ICDCdata {
  private int castnumber;
  private double latitude;
  private double longitude;
  private short year;
  private short month;
  private short day;
  private int rows;
  private int probeType;
  private String country;
  private List<Depth> depths = new ArrayList<>();
  private List<Integer> failures = new ArrayList<>();

  private Map<String, Integer> probeTypes = ImmutableMap.<String, Integer>builder()
      .put("OSD", Integer.valueOf(7))
      .put("CTD", Integer.valueOf(4))
      .put("PFL", Integer.valueOf(9))
      .put("APB", Integer.valueOf(13))
      .put("MBT", Integer.valueOf(1))
      .put("XBT", Integer.valueOf(2))
      .build();

  public ICDCdata(String header) {
    String[] fields = header.trim().split("\\s+");
    String probeType = fields[7].substring(fields[7].length() - 3);
    this.probeType = probeTypes.get(probeType);
    this.country = "JP";
    this.castnumber = Integer.parseInt(fields[1]);
    this.latitude = Double.parseDouble(fields[2]);
    this.longitude = Double.parseDouble(fields[3]);
    this.year = Short.parseShort(fields[4]);
    this.month = Short.parseShort(fields[5]);
    this.day = Short.parseShort(fields[6]);
    this.rows = Integer.parseInt(fields[7].substring(0, fields[7].length() - 3));

  }

  public int getCastnumber() {
    return castnumber;
  }

  public double getLatitude() {
    return latitude;
  }

  public double getLongitude() {
    return longitude;
  }

  public short getYear() {
    return year;
  }

  public short getMonth() {
    return month;
  }

  public short getDay() {
    return day;
  }

  public int getRows() {
    return rows;
  }

  public int getProbeType() {
    return probeType;
  }

  public String getCountry() {
    return country;
  }

  public List<Depth> getDepths() {
    return depths;
  }

  public void addDepth(int i, String line) {
    String[] data = line.trim().split("\\s+");
    if (data[2].equals("1")) {
      failures.add(i);
    }
    depths.add(Depth.builder().withDepth(Double.parseDouble(data[0]))
        .withData(Collections.singletonList(ProfileData.builder()
            .withVariableCode(TEMPERATURE).withValue(Double.parseDouble(data[1]))
            .build()))
        .build());
  }

  public List<Integer> getFailures() {
    return failures;
  }

  public boolean isPass() {
    if (this.failures.size() > 0) {
      return false;
    }
    return true;
  }

}
