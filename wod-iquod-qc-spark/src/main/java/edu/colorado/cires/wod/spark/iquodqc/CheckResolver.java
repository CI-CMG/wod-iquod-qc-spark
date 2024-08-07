package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

public class CheckResolver {


  public static class ParentChildren {
    private final String parent;
    private final Set<String> children = new LinkedHashSet<>();

    public ParentChildren(String parent) {
      this.parent = parent;
    }

    public String getParent() {
      return parent;
    }

    public Set<String> getChildren() {
      return children;
    }

    @Override
    public String toString() {
      return "ParentChildren{" +
          "parent='" + parent + '\'' +
          ", children=" + children +
          '}';
    }
  }

  public static List<ParentChildren> getParentChildren(Set<String> checksToRun) {
    List<CastCheck> checks = getChecks(checksToRun, false, null);
    Map<String, ParentChildren> parentChildren = new LinkedHashMap<>();
    for (CastCheck check : checks) {
      parentChildren.put(check.getName(), new ParentChildren(check.getName()));
      Collection<String> dependsOn = check.dependsOn();
      for (String d : dependsOn) {
        ParentChildren pc = parentChildren.get(d);
        pc.getChildren().add(check.getName());
      }
    }
    return new ArrayList<>(parentChildren.values());
  }

  public static List<CastCheck> getChecks(Set<String> checksToRun, boolean singleTest, @Nullable Properties properties) {
    CastCheckInitializationContext initContext = null;
    if (properties != null) {
      initContext = new CastCheckInitializationContext() {
        @Override
        public Properties getProperties() {
          return properties;
        }
      };
    }
    Map<String, CastCheck> checks = loadChecks(checksToRun, singleTest, initContext);
    if (singleTest) {
      return new ArrayList<>(checks.values());
    }
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

  private static Map<String, CastCheck> loadChecks(Set<String> checksToRun, boolean singleTest, @Nullable CastCheckInitializationContext initContext) {
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
    } if (singleTest) {
      for (String checkToRun : checksToRun) {
        if (allChecks.containsKey(checkToRun)) {
          checksToRunWithDependencies.add(checkToRun);
        } else {
          throw new IllegalArgumentException("Invalid check '" + checkToRun + "'");
        }
      }
    } else {
      for (String checkName : checksToRun) {
        updateChecks(checksToRunWithDependencies, allChecks, allChecks.get(checkName));
      }
    }
    Map<String, CastCheck> checks = new HashMap<>();
    for (String checkName : checksToRunWithDependencies) {
      CastCheck check = allChecks.get(checkName);
      if (initContext != null) {
        check.initialize(initContext);
      }
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
