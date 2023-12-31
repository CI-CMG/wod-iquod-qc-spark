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

public class CastCheckResult implements Serializable {

  private static final long serialVersionUID = 0L;

  public static StructType structType() {
    return new StructType(new StructField[]{
        new StructField("castNumber", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("passed", DataTypes.BooleanType, false, Metadata.empty()),
        new StructField("filtered", DataTypes.BooleanType, false, Metadata.empty()),
        new StructField("filterReason", DataTypes.StringType, true, Metadata.empty()),
        new StructField("failedDepths", DataTypes.createArrayType(DataTypes.IntegerType), true, Metadata.empty()),
    });
  }

  public Row asRow() {
    return new GenericRowWithSchema(new Object[]{castNumber, passed, filtered, filterReason, failedDepths}, structType());
  }

  private int castNumber;
  private boolean passed;
  private boolean filtered;
  private String filterReason;
  private List<Integer> failedDepths;

  private CastCheckResult(int castNumber, boolean passed, boolean filtered, String filterReason, List<Integer> failedDepths) {
    this.castNumber = castNumber;
    this.passed = passed;
    this.filtered = filtered;
    this.filterReason = filterReason;
    this.failedDepths = Collections.unmodifiableList(failedDepths);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CastCheckResult result = (CastCheckResult) o;
    return castNumber == result.castNumber && passed == result.passed && filtered == result.filtered && Objects.equals(filterReason,
        result.filterReason) && Objects.equals(failedDepths, result.failedDepths);
  }

  @Override
  public int hashCode() {
    return Objects.hash(castNumber, passed, filtered, filterReason, failedDepths);
  }

  @Override
  public String toString() {
    return "CastCheckResult{" +
        "castNumber=" + castNumber +
        ", passed=" + passed +
        ", filtered=" + filtered +
        ", filterReason='" + filterReason + '\'' +
        ", failedDepths=" + failedDepths +
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
    private List<Integer> failedDepths = new ArrayList<>(0);

    private Builder() {

    }

    private Builder(CastCheckResult orig) {
      castNumber = orig.castNumber;
      passed = orig.passed;
      filtered = orig.filtered;
      filterReason = orig.filterReason;
      failedDepths = new ArrayList<>(orig.failedDepths);
    }

    private Builder(Row row) {
      this.castNumber = row.getAs("castNumber");
      this.passed = row.getAs("passed");
      this.filtered = row.getAs("filtered");
      this.filterReason = row.getAs("filterReason");
      this.failedDepths = row.getList(row.fieldIndex("failedDepths"));
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

    public Builder withFailedDepths(List<Integer> failedDepths) {
      this.failedDepths = failedDepths == null ? new ArrayList<>(0) : new ArrayList<>(failedDepths);
      return this;
    }

    public CastCheckResult build() {
      return new CastCheckResult(castNumber, passed, filtered, filterReason, failedDepths);
    }
  }
}
