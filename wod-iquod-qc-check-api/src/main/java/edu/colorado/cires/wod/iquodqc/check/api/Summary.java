package edu.colorado.cires.wod.iquodqc.check.api;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Summary implements Serializable {
  
  private static final long serialVersionUID = 0L;
  
  public static StructType structType() {
    return new StructType(new StructField[]{
        new StructField("dataset", DataTypes.StringType, false, Metadata.empty()),
        new StructField("year", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("exceptionCount", DataTypes.LongType, false, Metadata.empty()),
        new StructField("failureCounts", DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType), false, Metadata.empty()),
        new StructField("totalProfiles", DataTypes.LongType, false, Metadata.empty())
    });
  }
  
  public Row asRow() {
    return new GenericRowWithSchema(new Object[]{
        dataset, year, exceptionCount, failureCounts, totalProfiles
    }, structType());
  }
  
  private String dataset;
  private int year;
  private Long exceptionCount;
  private Map<String, Long> failureCounts;
  private Long totalProfiles;

  private Summary(String dataset, int year, Long exceptionCount, Map<String, Long> failureCounts, Long totalProfiles) {
    this.dataset = Objects.requireNonNull(dataset, "dataset must not be null");
    this.year = year;
    this.exceptionCount = Objects.requireNonNull(exceptionCount, "exceptionCount must not be null");
    this.failureCounts = Collections.unmodifiableMap(Objects.requireNonNull(failureCounts, "failureCounts must not be null"));
    this.totalProfiles = Objects.requireNonNull(totalProfiles, "totalProfiles must not be null");
  }

  @Deprecated
  public Summary() {
  }

  public Long getExceptionCount() {
    return exceptionCount;
  }

  @Deprecated
  public void setExceptionCount(Long exceptionCount) {
    this.exceptionCount = exceptionCount;
  }

  public Map<String, Long> getFailureCounts() {
    return failureCounts;
  }

  @Deprecated
  public void setFailureCounts(Map<String, Long> failureCounts) {
    this.failureCounts = failureCounts;
  }

  public Long getTotalProfiles() {
    return totalProfiles;
  }

  @Deprecated
  public void setTotalProfiles(Long totalProfiles) {
    this.totalProfiles = totalProfiles;
  }

  public String getDataset() {
    return dataset;
  }

  @Deprecated
  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public int getYear() {
    return year;
  }

  @Deprecated
  public void setYear(int year) {
    this.year = year;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Summary)) {
      return false;
    }
    Summary summary = (Summary) o;
    return year == summary.year && Objects.equals(dataset, summary.dataset) && Objects.equals(exceptionCount,
        summary.exceptionCount) && Objects.equals(failureCounts, summary.failureCounts) && Objects.equals(totalProfiles,
        summary.totalProfiles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataset, year, exceptionCount, failureCounts, totalProfiles);
  }

  @Override
  public String toString() {
    return "Summary{" +
        "dataset='" + dataset + '\'' +
        ", year=" + year +
        ", exceptionCount=" + exceptionCount +
        ", failureCounts=" + failureCounts +
        ", totalProfiles=" + totalProfiles +
        '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Row row) {
    return new Builder(row);
  }

  public static Builder builder(Summary original) {
    return new Builder(original);
  }
  
  public static class Builder {
    private String dataset;
    private int year;
    private Long exceptionCount;
    private Map<String, Long> failureCounts = new HashMap<>(0);
    private Long totalProfiles;
    
    private Builder() {}
    
    private Builder(Row row) {
      dataset = row.getString(row.fieldIndex("dataset"));
      year = row.getInt(row.fieldIndex("year"));
      exceptionCount = row.getLong(row.fieldIndex("exceptionCount"));
      failureCounts = row.getJavaMap(row.fieldIndex("failureCounts"));
      totalProfiles = row.getLong(row.fieldIndex("totalProfiles"));
    }
    
    private Builder(Summary original) {
      dataset = original.dataset;
      year = original.year;
      exceptionCount = original.exceptionCount;
      failureCounts = new HashMap<>(original.failureCounts);
      totalProfiles = original.totalProfiles;
    }
    
    public Builder withDataset(String dataset) {
      this.dataset = dataset;
      return this;
    }
    
    public Builder withYear(int year) {
      this.year = year;
      return this;
    }
    
    public Builder withExceptionCount(Long exceptionCount) {
      this.exceptionCount = exceptionCount;
      return this;
    }
    
    public Builder withFailureCounts(Map<String, Long> failureCounts) {
      if (failureCounts == null) {
        failureCounts = new HashMap<>(0);
      }
      this.failureCounts = failureCounts;
      return this;
    }
    
    public Builder withTotalProfiles(Long totalProfiles) {
      this.totalProfiles = totalProfiles;
      return this;
    }
    
    public Summary build() {
      return new Summary(dataset, year, exceptionCount, failureCounts, totalProfiles);
    }
  }
}
