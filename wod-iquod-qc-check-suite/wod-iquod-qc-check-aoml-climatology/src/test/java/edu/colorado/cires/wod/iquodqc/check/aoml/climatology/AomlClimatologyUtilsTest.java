package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class AomlClimatologyUtilsTest {

  @Test
  public void testClosestIndex() {
    assertEquals(2, AomlClimatologyUtils.closestIndex(new float[]{10f, 11f, 12f, 13f, 14f}, 12.1));
    assertEquals(2, AomlClimatologyUtils.closestIndex(new float[]{10f, 11f, 12f, 13f, 14f}, 11.9));
    assertEquals(2, AomlClimatologyUtils.closestIndex(new float[]{10f, 11f, 12f, 13f, 14f}, 12d));
    assertEquals(2, AomlClimatologyUtils.closestIndex(new float[]{10f, 11f, 12f, 12f, 14f}, 12.1));
    assertEquals(0, AomlClimatologyUtils.closestIndex(new float[]{12f, 10f, 11f, 13f, 14f}, 12.1));
    assertEquals(4, AomlClimatologyUtils.closestIndex(new float[]{10f, 11f, 13f, 14f, 12f}, 12.1));
    assertEquals(4, AomlClimatologyUtils.closestIndex(new float[]{10f, 11f, 12f, 13f, 14f}, 100d));
    assertEquals(1, AomlClimatologyUtils.closestIndex(new float[]{14f, 12f, 10f, 11f, 13f}, 12d));
  }

  @Test
  public void testGetIndexAndNext() {
    assertArrayEquals(new int[]{2, 3}, AomlClimatologyUtils.getIndexAndNext(new float[]{10, 11, 12, 13, 14}, 12.1));
    assertArrayEquals(new int[]{3, 4}, AomlClimatologyUtils.getIndexAndNext(new float[]{10, 11, 12, 13, 14}, 15));
    assertArrayEquals(new int[]{3, 4}, AomlClimatologyUtils.getIndexAndNext(new float[]{10, 11, 13, 12, 14}, 12.1));
    assertArrayEquals(new int[]{3, 4}, AomlClimatologyUtils.getIndexAndNext(new float[]{10, 11, 13, 12, 10}, 12.1));
    assertArrayEquals(new int[]{0, 1}, AomlClimatologyUtils.getIndexAndNext(new float[]{12, 13, 12, 13}, 12.1));
  }

}