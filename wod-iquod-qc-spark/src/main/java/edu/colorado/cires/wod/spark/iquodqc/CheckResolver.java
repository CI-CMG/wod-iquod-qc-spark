package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

public class CheckResolver {


  public static List<CastCheck> getChecks(Set<String> checksToRun, Properties properties) {
    CastCheckInitializationContext initContext = new CastCheckInitializationContext() {
      @Override
      public Properties getProperties() {
        return properties;
      }
    };
    Map<String, CastCheck> checks = loadChecks(checksToRun, initContext);
    DirectedAcyclicGraph<CastCheck, DefaultEdge> dag = planChecks(checks);
    List<CastCheck> order = new ArrayList<>(checks.size());
    Iterator<CastCheck> it = dag.iterator();
    while (it.hasNext()) {
      order.add(it.next());
    }
    Collections.reverse(order);
    return order;
  }

  private static void updateChecks(Set<String> checksToRunWithDependencies, Map<String, CastCheck> allChecks, CastCheck check) {
    checksToRunWithDependencies.add(check.getName());
    for (String checkName : check.dependsOn()) {
      updateChecks(checksToRunWithDependencies, allChecks, allChecks.get(checkName));
    }
  }

  private static Map<String, CastCheck> loadChecks(Set<String> checksToRun, CastCheckInitializationContext initContext) {
    Map<String, CastCheck> allChecks = new HashMap<>();
    Set<String> checksToRunWithDependencies = new HashSet<>();
    for (CastCheck check : ServiceLoader.load(CastCheck.class)) {
      if (allChecks.get(check.getName()) != null) {
        throw new IllegalArgumentException("Duplicate check with name '" + check.getName() + "' detected");
      }
      allChecks.put(check.getName(), check);
    }
    if (checksToRun.isEmpty()) {
      checksToRunWithDependencies.addAll(allChecks.keySet());
    } else {
      for (String checkName : checksToRun) {
        updateChecks(checksToRunWithDependencies, allChecks, allChecks.get(checkName));
      }
    }
    Map<String, CastCheck> checks = new HashMap<>();
    for (String checkName : checksToRunWithDependencies) {
      CastCheck check = allChecks.get(checkName);
      check.initialize(initContext);
      checks.put(checkName, check);
    }
    return checks;
  }

  private static DirectedAcyclicGraph<CastCheck, DefaultEdge> planChecks(Map<String, CastCheck> checks) {
    DirectedAcyclicGraph<CastCheck, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    checks.values().forEach(dag::addVertex);
    checks.values().forEach(check -> check.dependsOn()
        .forEach(depName ->
            dag.addEdge(check, Optional.ofNullable(checks.get(depName))
                .orElseThrow(() -> new IllegalArgumentException(
                    "Unable to find check named '" + depName + "' defined as a dependency for check '" + check.getName() + "'")))));
    return dag;
  }

}
