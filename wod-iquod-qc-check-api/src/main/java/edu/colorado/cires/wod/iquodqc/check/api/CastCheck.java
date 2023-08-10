package edu.colorado.cires.wod.iquodqc.check.api;

import java.util.Collection;
import java.util.Collections;
import org.apache.spark.sql.Dataset;

public interface CastCheck {

  String getName();

  Dataset<CastCheckResult> joinResultDataset(CastCheckContext context);

  default Collection<String> dependsOn() {
    return Collections.emptySet();
  }

  default void initialize(CastCheckInitializationContext initContext) {

  }

}
