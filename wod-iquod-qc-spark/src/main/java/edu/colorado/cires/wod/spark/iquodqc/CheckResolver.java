package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

public class CheckResolver {

  private final Map<String, CastCheck> checks;

  private final Graph<CastCheck, DefaultEdge> dag;

  public CheckResolver(Set<String> checksToRun, Properties properties) {
    CastCheckInitializationContext initContext = new CastCheckInitializationContext() {
      @Override
      public Properties getProperties() {
        return properties;
      }
    };
    checks = Collections.unmodifiableMap(loadChecks(checksToRun, initContext));
    dag = planChecks();
  }

  public synchronized Collection<CastCheck> getRunnableChecks() {
    return dag.vertexSet().stream().filter(check -> dag.degreeOf(check) == 0).collect(Collectors.toSet());
  }

  public synchronized Collection<CastCheck> completedCheck(CastCheck check) {
    dag.removeVertex(check);
    return getRunnableChecks();
  }

  private static Map<String, CastCheck> loadChecks(Set<String> checksToRun, CastCheckInitializationContext initContext) {
    Map<String, CastCheck> checks = new HashMap<>();
    for (CastCheck check : ServiceLoader.load(CastCheck.class)) {
      if (checks.get(check.getName()) != null) {
        throw new IllegalArgumentException("Duplicate check with name '" + check.getName() + "' detected");
      }
      if (checksToRun.isEmpty() || checksToRun.contains(check.getName())) {
        check.initialize(initContext);
        checks.put(check.getName(), check);
      }
    }
    return checks;
  }

  private Graph<CastCheck, DefaultEdge> planChecks() {
    Graph<CastCheck, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    checks.values().forEach(dag::addVertex);
    checks.values().forEach( check -> check.dependsOn()
        .forEach(depName ->
            dag.addEdge(check, Optional.ofNullable(checks.get(depName))
                .orElseThrow(() -> new IllegalArgumentException("Unable to find check named '" + depName + "' defined as a dependency for check '" + check.getName() + "'")))));
    return dag;
  }

}
