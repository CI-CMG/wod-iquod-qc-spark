package edu.colorado.cires.wod.iquodqc.check.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CastCheckResult implements Serializable {

  private static final long serialVersionUID = 0L;

  public static StructType structType() {
    return new StructType(new StructField[]{
        new StructField("castNumber", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("passed", DataTypes.BooleanType, false, Metadata.empty()),
        new StructField("filtered", DataTypes.BooleanType, false, Metadata.empty()),
        new StructField("filterReason", DataTypes.StringType, true, Metadata.empty()),
        new StructField("error", DataTypes.BooleanType, false, Metadata.empty()),
        new StructField("errorMessage", DataTypes.StringType, true, Metadata.empty()),
        new StructField("failedDepths", DataTypes.createArrayType(DataTypes.IntegerType), true, Metadata.empty()),
        new StructField("iquodFlags", DataTypes.createArrayType(DataTypes.IntegerType), true, Metadata.empty()),
        new StructField(
            "dependsOnFailedDepths",
            DataTypes.createMapType(
                DataTypes.StringType,
                DataTypes.createArrayType(DataTypes.IntegerType)
            ),
            true,
            Metadata.empty()
        ),
        new StructField(
            "dependsOnFailedChecks",
            DataTypes.createMapType(
                DataTypes.StringType,
                DataTypes.createArrayType(DataTypes.StringType)
            ),
            true,
            Metadata.empty()
        ),
        new StructField("signal", DataTypes.createArrayType(DataTypes.DoubleType), true, Metadata.empty())
    });
  }

  public Row asRow() {
    return new GenericRowWithSchema(
        new Object[]{
            castNumber,
            passed,
            filtered,
            filterReason,
            error,
            errorMessage,
            failedDepths,
            iquodFlags,
            dependsOnFailedDepths,
            dependsOnFailedChecks,
            signal
        },
        structType()
    );
  }

  private int castNumber;
  private boolean passed;
  private boolean filtered;
  private String filterReason;
  private boolean error;
  private String errorMessage;
  private List<Integer> failedDepths;
  private List<Integer> iquodFlags;
  private Map<String, List<Integer>> dependsOnFailedDepths;
  private Map<String, List<String>> dependsOnFailedChecks;
  private List<Double> signal;

  private CastCheckResult(int castNumber, boolean passed, boolean filtered, String filterReason, boolean error, String errorMessage, List<Integer> failedDepths, List<Integer> iquodFlags, Map<String, List<Integer>> dependsOnFailedDepths, Map<String, List<String>> dependsOnFailedChecks, List<Double> signal) {
    this.castNumber = castNumber;
    this.passed = passed;
    this.filtered = filtered;
    this.filterReason = filterReason;
    this.error = error;
    this.errorMessage = errorMessage;
    this.failedDepths = Collections.unmodifiableList(failedDepths);
    this.iquodFlags = Collections.unmodifiableList(iquodFlags);
    this.dependsOnFailedDepths = Collections.unmodifiableMap(dependsOnFailedDepths);
    this.dependsOnFailedChecks = Collections.unmodifiableMap(dependsOnFailedChecks);
    this.signal = Collections.unmodifiableList(signal);
  }

  @Deprecated
  public CastCheckResult() {

  }

  public int getCastNumber() {
    return castNumber;
  }

  @Deprecated
  public void setCastNumber(int castNumber) {
    this.castNumber = castNumber;
  }

  public boolean isPassed() {
    return passed;
  }

  @Deprecated
  public void setPassed(boolean passed) {
    this.passed = passed;
  }

  public boolean isFiltered() {
    return filtered;
  }

  @Deprecated
  public void setFiltered(boolean filtered) {
    this.filtered = filtered;
  }

  public String getFilterReason() {
    return filterReason;
  }

  @Deprecated
  public void setFilterReason(String filterReason) {
    this.filterReason = filterReason;
  }

  public boolean isError() {
    return error;
  }

  @Deprecated
  public void setError(boolean error) {
    this.error = error;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Deprecated
  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public List<Integer> getFailedDepths() {
    return failedDepths;
  }

  @Deprecated
  public void setFailedDepths(List<Integer> failedDepths) {
    if (failedDepths == null) {
      failedDepths = new ArrayList<>(0);
    }
    this.failedDepths = failedDepths;
  }

  public List<Integer> getIquodFlags() {
    return iquodFlags;
  }

  @Deprecated
  public void setIquodFlags(List<Integer> iquodFlags) {
    this.iquodFlags = iquodFlags;
  }

  public Map<String, List<Integer>> getDependsOnFailedDepths() {
    return dependsOnFailedDepths;
  }

  @Deprecated
  public void setDependsOnFailedDepths(Map<String, List<Integer>> dependsOn) {
    this.dependsOnFailedDepths = dependsOn;
  }

  public Map<String, List<String>> getDependsOnFailedChecks() {
    return dependsOnFailedChecks;
  }

  @Deprecated
  public void setDependsOnFailedChecks(Map<String, List<String>> dependsOnFailedChecks) {
    this.dependsOnFailedChecks = dependsOnFailedChecks;
  }

  public List<Double> getSignal() {
    return signal;
  }

  @Deprecated
  public void setSignal(List<Double> signal) {
    if (signal == null) {
      signal = new ArrayList<>(0);
    }
    this.signal = signal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CastCheckResult result = (CastCheckResult) o;
    return castNumber == result.castNumber && passed == result.passed && filtered == result.filtered && error == result.error
        && Objects.equals(filterReason, result.filterReason) && Objects.equals(errorMessage, result.errorMessage)
        && Objects.equals(failedDepths, result.failedDepths) && Objects.equals(iquodFlags, result.iquodFlags)
        && Objects.equals(dependsOnFailedDepths, result.dependsOnFailedDepths) && Objects.equals(dependsOnFailedChecks,
        result.dependsOnFailedChecks) && Objects.equals(signal, result.signal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(castNumber, passed, filtered, filterReason, error, errorMessage, failedDepths, iquodFlags, dependsOnFailedDepths,
        dependsOnFailedChecks, signal);
  }

  @Override
  public String toString() {
    return "CastCheckResult{" +
        "castNumber=" + castNumber +
        ", passed=" + passed +
        ", filtered=" + filtered +
        ", filterReason='" + filterReason + '\'' +
        ", error=" + error +
        ", errorMessage='" + errorMessage + '\'' +
        ", failedDepths=" + failedDepths +
        ", iquodFlags=" + iquodFlags +
        ", dependsOnFailedDepths=" + dependsOnFailedDepths +
        ", dependsOnFailedChecks=" + dependsOnFailedChecks +
        ", signal=" + signal +
        '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(CastCheckResult orig) {
    return new Builder(orig);
  }

  public static Builder builder(Row row) {
    return new Builder(row);
  }

  public static class Builder {

    private int castNumber;
    private boolean passed;
    private boolean filtered;
    private String filterReason;
    private boolean error;
    private String errorMessage;
    private List<Integer> failedDepths = new ArrayList<>(0);
    private List<Integer> iquodFlags = new ArrayList<>(0);
    private Map<String, List<Integer>> dependsOnFailedDepths = new HashMap<>(0);
    private Map<String, List<String>> dependsOnFailedChecks = new HashMap<>(0);
    private List<Double> signal = new ArrayList<>(0);

    private Builder() {

    }

    private Builder(CastCheckResult orig) {
      castNumber = orig.castNumber;
      passed = orig.passed;
      filtered = orig.filtered;
      filterReason = orig.filterReason;
      error = orig.error;
      errorMessage = orig.errorMessage;
      failedDepths = new ArrayList<>(orig.failedDepths);
      iquodFlags = new ArrayList<>(orig.iquodFlags);
      dependsOnFailedDepths = new HashMap<>(orig.dependsOnFailedDepths);
      dependsOnFailedChecks = new HashMap<>(orig.dependsOnFailedChecks);
      signal = new ArrayList<>(orig.signal);
    }

    private Builder(Row row) {
      castNumber = row.getAs("castNumber");
      passed = row.getAs("passed");
      filtered = row.getAs("filtered");
      filterReason = row.getAs("filterReason");
      error = row.getAs("error");
      errorMessage = row.getAs("errorMessage");
      failedDepths = row.getList(row.fieldIndex("failedDepths"));
      iquodFlags = row.getList(row.fieldIndex("iquodFlags"));
      dependsOnFailedDepths = row.getJavaMap(row.fieldIndex("dependsOnFailedDepths"));
      dependsOnFailedChecks = row.getJavaMap(row.fieldIndex("dependsOnFailedChecks"));
      signal = row.getList(row.fieldIndex("signal"));
    }

    public Builder withCastNumber(int castNumber) {
      this.castNumber = castNumber;
      return this;
    }

    public Builder withPassed(boolean passed) {
      this.passed = passed;
      return this;
    }

    public Builder withFiltered(boolean filtered) {
      this.filtered = filtered;
      return this;
    }

    public Builder withFilterReason(String filterReason) {
      this.filterReason = filterReason;
      return this;
    }

    public Builder withError(boolean error) {
      this.error = error;
      return this;
    }

    public Builder withErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    public Builder withFailedDepths(List<Integer> failedDepths) {
      this.failedDepths = failedDepths == null ? new ArrayList<>(0) : new ArrayList<>(failedDepths);
      return this;
    }

    public Builder withIquodFlags(List<Integer> iquodFlags) {
      this.iquodFlags = iquodFlags;
      return this;
    }

    public Builder withDependsOnFailedDepths(Map<String, List<Integer>> dependsOnFailedDepths) {
      this.dependsOnFailedDepths = dependsOnFailedDepths;
      return this;
    }

    public Builder withDependsOnFailedChecks(Map<String, List<String>> dependsOnFailedChecks) {
      this.dependsOnFailedChecks = dependsOnFailedChecks;
      return this;
    }

    public Builder withSignal(List<Double> signal) {
      if (signal == null) {
        signal = Collections.emptyList();
      }
      this.signal = signal;
      return this;
    }

    public CastCheckResult build() {
      return new CastCheckResult(castNumber, passed, filtered, filterReason, error, errorMessage, failedDepths, iquodFlags, dependsOnFailedDepths, dependsOnFailedChecks, signal);
    }
  }
}
