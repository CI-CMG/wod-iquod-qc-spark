package edu.colorado.cires.wod.iquodqc.check.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Failures implements Serializable {
  
  private static final long serialVersionUID = 0L;
  
  public static StructType structType() {
    return new StructType(new StructField[]{
        new StructField("castNumber", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("dataset", DataTypes.StringType, false, Metadata.empty()),
        new StructField("year", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("exception", DataTypes.StringType, true, Metadata.empty()),
        new StructField("iquodFlags", DataTypes.createArrayType(DataTypes.IntegerType), false, Metadata.empty()),
        new StructField("profileFailures", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()),
        new StructField("depthFailures", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)), false, Metadata.empty())
    });
  }
  
  public Row asRow() {
    return new GenericRowWithSchema(new Object[]{
        castNumber, dataset, year, exception, iquodFlags, profileFailures, depthFailures
    }, structType());
  }
  
  private int castNumber;
  private String dataset;
  private int year;
  private String exception;
  private List<Integer> iquodFlags;
  private List<String> profileFailures;
  private List<List<String>> depthFailures;

  private Failures(int castNumber, String dataset, int year, String exception, List<Integer> iquodFlags, List<String> profileFailures, List<List<String>> depthFailures) {
    this.castNumber = castNumber;
    this.dataset = Objects.requireNonNull(dataset, "dataset must not be null");
    this.year = year;
    this.exception = exception;
    this.iquodFlags = Collections.unmodifiableList(iquodFlags);
    this.profileFailures = Collections.unmodifiableList(Objects.requireNonNull(profileFailures, "profileFailures must not be null"));
    this.depthFailures = Collections.unmodifiableList(depthFailures);
  }

  @Deprecated
  public Failures() {
  }

  public String getException() {
    return exception;
  }

  @Deprecated
  public void setException(String exception) {
    this.exception = exception;
  }

  public List<Integer> getIquodFlags() {
    return iquodFlags;
  }

  @Deprecated
  public void setIquodFlags(List<Integer> iquodFlags) {
    this.iquodFlags = iquodFlags;
  }

  public List<String> getProfileFailures() {
    return profileFailures;
  }

  @Deprecated
  public void setProfileFailures(List<String> profileFailures) {
    this.profileFailures = profileFailures;
  }

  public List<List<String>> getDepthFailures() {
    return depthFailures;
  }

  @Deprecated
  public void setDepthFailures(List<List<String>> depthFailures) {
    this.depthFailures = depthFailures;
  }

  public int getCastNumber() {
    return castNumber;
  }

  @Deprecated
  public void setCastNumber(int castNumber) {
    this.castNumber = castNumber;
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
    if (!(o instanceof Failures)) {
      return false;
    }
    Failures failures = (Failures) o;
    return castNumber == failures.castNumber && year == failures.year && Objects.equals(dataset, failures.dataset)
        && Objects.equals(exception, failures.exception) && Objects.equals(iquodFlags, failures.iquodFlags)
        && Objects.equals(profileFailures, failures.profileFailures) && Objects.equals(depthFailures, failures.depthFailures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(castNumber, dataset, year, exception, iquodFlags, profileFailures, depthFailures);
  }

  @Override
  public String toString() {
    return "Failures{" +
        "castNumber=" + castNumber +
        ", dataset='" + dataset + '\'' +
        ", year=" + year +
        ", exception='" + exception + '\'' +
        ", iquodFlags=" + iquodFlags +
        ", profileFailures=" + profileFailures +
        ", depthFailures=" + depthFailures +
        '}';
  }

  public static Builder builder() {
    return new Builder();
  }
  
  public static Builder builder(Row row) {
    return new Builder(row);
  }
  
  public static Builder builder(Failures original) {
    return new Builder(original);
  }
  
  public static class Builder {
    private int castNumber;
    private String dataset;
    private int year;
    private String exception;
    private List<Integer> iquodFlags = new ArrayList<>(0);
    private List<String> profileFailures = new ArrayList<>(0);
    private List<List<String>> depthFailures = new ArrayList<>(0);
    
    private Builder() {}
    private Builder(Row row) {
      castNumber = row.getInt(row.fieldIndex("castNumber"));
      dataset = row.getString(row.fieldIndex("dataset"));
      year = row.getInt(row.fieldIndex("year"));
      exception = row.getString(row.fieldIndex("exception"));
      iquodFlags = row.getList(row.fieldIndex("iquodFlags"));
      profileFailures = row.getList(row.fieldIndex("profileFailures"));
      depthFailures = row.getList(row.fieldIndex("depthFailures"));
    }
    private Builder(Failures failures) {
      castNumber = failures.castNumber;
      dataset = failures.dataset;
      year = failures.year;
      exception = failures.exception;
      iquodFlags = new ArrayList<>(failures.iquodFlags);
      profileFailures = new ArrayList<>(failures.profileFailures);
      depthFailures = new ArrayList<>(failures.depthFailures);
    }
    
    public Builder withCastNumber(int castNumber) {
      this.castNumber = castNumber;
      return this;
    }
    
    public Builder withDataset(String dataset) {
      this.dataset = dataset;
      return this;
    }
    
    public Builder withYear(int year) {
      this.year = year;
      return this;
    }
    
    public Builder withException(String exception) {
      this.exception = exception;
      return this;
    }
    
    public Builder withIquodFlags(List<Integer> iquodFlags) {
      if (iquodFlags == null) {
        iquodFlags = new ArrayList<>(0);
      }
      this.iquodFlags = iquodFlags;
      return this;
    }
    
    public Builder withProfileFailures(List<String> profileFailures) {
      if (profileFailures == null) {
        profileFailures = new ArrayList<>(0);
      }
      this.profileFailures = profileFailures;
      return this;
    }
    
    public Builder withDepthFailures(List<List<String>> depthFailures) {
      if (depthFailures == null) {
        depthFailures = new ArrayList<>(0);
      }
      this.depthFailures = depthFailures;
      return this;
    }
    
    public Failures build() {
      return new Failures(castNumber, dataset, year, exception, iquodFlags, profileFailures, depthFailures);
    }
  }
}
