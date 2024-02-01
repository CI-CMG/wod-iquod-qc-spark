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
        new StructField("exception", DataTypes.StringType, true, Metadata.empty()),
        new StructField("iquodFlags", DataTypes.createArrayType(DataTypes.IntegerType), false, Metadata.empty()),
        new StructField("profileFailures", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()),
        new StructField("depthFailures", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)), false, Metadata.empty())
    });
  }
  
  public Row asRow() {
    return new GenericRowWithSchema(new Object[]{
        exception, iquodFlags, profileFailures, depthFailures
    }, structType());
  }
  
  private String exception;
  private List<Integer> iquodFlags;
  private List<String> profileFailures;
  private List<List<String>> depthFailures;

  private Failures(String exception, List<Integer> iquodFlags, List<String> profileFailures, List<List<String>> depthFailures) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Failures)) {
      return false;
    }
    Failures failures = (Failures) o;
    return Objects.equals(exception, failures.exception) && Objects.equals(iquodFlags, failures.iquodFlags)
        && Objects.equals(profileFailures, failures.profileFailures) && Objects.equals(depthFailures, failures.depthFailures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exception, iquodFlags, profileFailures, depthFailures);
  }

  @Override
  public String toString() {
    return "Failures{" +
        "exception='" + exception + '\'' +
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
    private String exception;
    private List<Integer> iquodFlags = new ArrayList<>(0);
    private List<String> profileFailures = new ArrayList<>(0);
    private List<List<String>> depthFailures = new ArrayList<>(0);
    
    private Builder() {}
    private Builder(Row row) {
      exception = row.getString(row.fieldIndex("exception"));
      iquodFlags = row.getList(row.fieldIndex("iquodFlags"));
      profileFailures = row.getList(row.fieldIndex("profileFailures"));
      depthFailures = row.getList(row.fieldIndex("depthFailures"));
    }
    private Builder(Failures failures) {
      exception = failures.exception;
      iquodFlags = new ArrayList<>(failures.iquodFlags);
      profileFailures = new ArrayList<>(failures.profileFailures);
      depthFailures = new ArrayList<>(failures.depthFailures);
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
      return new Failures(exception, iquodFlags, profileFailures, depthFailures);
    }
  }
}
