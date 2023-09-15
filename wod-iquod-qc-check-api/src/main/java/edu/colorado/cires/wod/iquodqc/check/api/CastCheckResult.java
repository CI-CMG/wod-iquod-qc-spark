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
        new StructField("failedDepths", DataTypes.createArrayType(DataTypes.IntegerType), true, Metadata.empty()),
    });
  }

  ;

  public Row asRow() {
    return new GenericRowWithSchema(new Object[]{castNumber, passed, failedDepths}, structType());
  }

  private int castNumber;
  private boolean passed;
  private List<Integer> failedDepths;

  private CastCheckResult(int castNumber, boolean passed, List<Integer> failedDepths) {
    this.castNumber = castNumber;
    this.passed = passed;
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
    return castNumber == result.castNumber && passed == result.passed && Objects.equals(failedDepths, result.failedDepths);
  }

  @Override
  public int hashCode() {
    return Objects.hash(castNumber, passed, failedDepths);
  }

  @Override
  public String toString() {
    return "CastCheckResult{" +
        "castNumber=" + castNumber +
        ", passed=" + passed +
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
    private List<Integer> failedDepths = new ArrayList<>(0);

    private Builder() {

    }

    private Builder(CastCheckResult orig) {
      castNumber = orig.castNumber;
      passed = orig.passed;
      failedDepths = new ArrayList<>(orig.failedDepths);
    }

    private Builder(Row row) {
      this.castNumber = row.getAs("castNumber");
      this.passed = row.getAs("passed");
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

    public Builder withFailedDepths(List<Integer> failedDepths) {
      this.failedDepths = failedDepths == null ? new ArrayList<>(0) : new ArrayList<>(failedDepths);
      return this;
    }

    public CastCheckResult build() {
      return new CastCheckResult(castNumber, passed, failedDepths);
    }
  }
}
