package edu.colorado.cires.wod.spark.iquodqc;

import java.util.Objects;

public class DagPruneEntry {

  private final String year;
  private final String dataset;
  private final String check;

  public DagPruneEntry(String year, String dataset, String check) {
    this.year = year;
    this.dataset = dataset;
    this.check = check;
  }

  public String getYear() {
    return year;
  }

  public String getDataset() {
    return dataset;
  }

  public String getCheck() {
    return check;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DagPruneEntry that = (DagPruneEntry) o;
    return Objects.equals(year, that.year) && Objects.equals(dataset, that.dataset) && Objects.equals(check, that.check);
  }

  @Override
  public int hashCode() {
    return Objects.hash(year, dataset, check);
  }

  @Override
  public String toString() {
    return "DagPruneEntry{" +
        "year='" + year + '\'' +
        ", dataset='" + dataset + '\'' +
        ", check='" + check + '\'' +
        '}';
  }
}
