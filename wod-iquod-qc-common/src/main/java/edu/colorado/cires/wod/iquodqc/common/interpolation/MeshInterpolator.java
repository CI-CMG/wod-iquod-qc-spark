package edu.colorado.cires.wod.iquodqc.common.interpolation;

import com.github.quickhull3d.Point3d;
import com.github.quickhull3d.QuickHull3D;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;
import org.apache.commons.geometry.euclidean.threed.Plane;
import org.apache.commons.geometry.euclidean.threed.Planes;
import org.apache.commons.geometry.euclidean.threed.Vector3D;
import org.apache.commons.geometry.euclidean.threed.line.Line3D;
import org.apache.commons.geometry.euclidean.threed.line.Lines3D;
import org.apache.commons.numbers.core.Precision;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

public class MeshInterpolator {

  private final double[] x;
  private final double[] y;
  private final double[] values;

  public MeshInterpolator(double[] x, double[] y, double[] values) {
    this.x = x;
    this.y = y;
    this.values = values;
  }

  public double interpolate(double targetX, double targetY) throws UnableToInterpolateException {
    if (values.length < 3) {
      throw new UnableToInterpolateException("Unable to interpolate with less than 3 points");
    } else if (values.length == 3) {
      return interpolatePlane(targetX, targetY);
    } else {
      return interpolateWithQuickHull(targetX, targetY);
    }
  }

  private double interpolatePlane(double targetX, double targetY) throws UnableToInterpolateException {
    Point point = JTSFactoryFinder.getGeometryFactory().createPoint(new Coordinate(targetX, targetY));
    Triangle triangle = new Triangle(
        new Point3d(x[0], y[0], values[0]),
        new Point3d(x[1], y[1], values[1]),
        new Point3d(x[2], y[2], values[2]));
    if (!triangle.asPolygon().intersects(point)) {
      throw new UnableToInterpolateException("Point [" + targetX + "," + targetY + "] was not inside of mesh");
    }
    return interpolatePlane(triangle, targetX, targetY);
  }

  private double interpolatePlane(Triangle triangle, double targetX, double targetY) {
    Plane plane = triangle.asPlane();
    Line3D xyLine = Lines3D.fromPoints(Vector3D.of(targetX, targetY, triangle.getMinZ()), Vector3D.of(targetX, targetY, triangle.getMaxZ()), Precision.doubleEquivalenceOfEpsilon(1e-6));
    return plane.intersection(xyLine).getZ();
  }

  private double interpolateWithQuickHull(double targetX, double targetY) throws UnableToInterpolateException {
    Point3d[] points = new Point3d[values.length];
    for (int i = 0; i < values.length; i++) {
      points[i] = new Point3d(x[i], y[i], values[i]);
    }

    QuickHull3D hull = new QuickHull3D();
    try {
      hull.build(points);
    } catch (Exception e){
      throw new UnableToInterpolateException(e.getMessage());
    }


    Point3d[] verticies = hull.getVertices();
    int[][] faces = hull.getFaces();
    List<Triangle> triangles = new ArrayList<>(faces.length);
    for (int[] indexes : faces) {
      triangles.add(new Triangle(verticies[indexes[0]], verticies[indexes[1]], verticies[indexes[2]]));
    }
    Triangle triangle = findTriangle(triangles, targetX, targetY);
    return interpolatePlane(triangle, targetX, targetY);
  }

  private Triangle findTriangle(List<Triangle> triangles, double targetX, double targetY) throws UnableToInterpolateException {
    Point point = JTSFactoryFinder.getGeometryFactory().createPoint(new Coordinate(targetX, targetY));
    for (Triangle triangle : triangles) {
      if (triangle.asPolygon().intersects(point)) {
        return triangle;
      }
    }
    throw new UnableToInterpolateException("Point [" + targetX + "," + targetY + "] was not inside of mesh");
  }

  private static class Triangle {
    private final Point3d pt1;
    private final Point3d pt2;
    private final Point3d pt3;

    private Triangle(Point3d pt1, Point3d pt2, Point3d pt3) {
      this.pt1 = pt1;
      this.pt2 = pt2;
      this.pt3 = pt3;
    }

    public Polygon asPolygon() {
      return JTSFactoryFinder.getGeometryFactory().createPolygon(
          new Coordinate[]{
              new Coordinate(pt1.x, pt1.y),
              new Coordinate(pt2.x, pt2.y),
              new Coordinate(pt3.x, pt3.y),
              new Coordinate(pt1.x, pt1.y)
          });
    }

    public Plane asPlane() {
      return Planes.fromPoints(
          Vector3D.of(pt1.x, pt1.y, pt1.z),
          Vector3D.of(pt2.x, pt2.y, pt2.z),
          Vector3D.of(pt3.x, pt3.y, pt3.z),
          Precision.doubleEquivalenceOfEpsilon(1e-6));
    }

    public double getMaxZ() {
      return DoubleStream.of(pt1.z, pt2.z, pt3.z).max().orElseThrow(() -> new IllegalStateException("Unable to determine max Z"));
    }

    public double getMinZ() {
      return DoubleStream.of(pt1.z, pt2.z, pt3.z).min().orElseThrow(() -> new IllegalStateException("Unable to determine min Z"));
    }
  }
}
