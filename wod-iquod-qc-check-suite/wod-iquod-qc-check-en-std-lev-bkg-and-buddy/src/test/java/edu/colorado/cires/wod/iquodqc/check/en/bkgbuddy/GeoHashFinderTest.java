package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

public class GeoHashFinderTest {

  @Test
  public void testNeighborsAtNorth() throws Exception {
    Set<String> hashes = GeoHashFinder.getNeighborsInDistance(-179.9999999, 89.999999D, 10D);
    assertEquals(256, hashes.size());
    assertTrue(hashes.contains("bpb"));
    assertTrue(hashes.contains("zzz"));
  }

  @Test
  public void testNeighborsAtSouth() throws Exception {
    Set<String> hashes = GeoHashFinder.getNeighborsInDistance(-179.9999999, -89.999999D, 10D);
    assertEquals(256, hashes.size());
    assertTrue(hashes.contains("000"));
    assertTrue(hashes.contains("pbp"));
  }

  @Test
  public void testAntimeridianWest() throws Exception {
    Set<String> hashes = GeoHashFinder.getNeighborsInDistance(-179D, 0D, 400000D);
    assertEquals(new TreeSet<>(
        Arrays.asList("2p2", "2p3", "2p6", "2p7", "2p8", "2p9", "2pb", "2pc", "2pd", "2pe", "2pf", "2pg", "800", "801", "802", "803", "804", "805",
            "806", "807", "808", "809", "80d", "80e", "rzq", "rzr", "rzw", "rzx", "rzy", "rzz", "xbn", "xbp", "xbq", "xbr", "xbw", "xbx")), hashes);
  }

  @Test
  public void testAntimeridianEast() throws Exception {
    Set<String> hashes = GeoHashFinder.getNeighborsInDistance(179D, 0D, 400000D);
    assertEquals(new TreeSet<>(
        Arrays.asList("2p2", "2p3", "2p8", "2p9", "2pb", "2pc", "800", "801", "802", "803", "808", "809", "rzk", "rzm", "rzq", "rzr", "rzs", "rzt",
            "rzu", "rzv", "rzw", "rzx", "rzy", "rzz", "xbh", "xbj", "xbk", "xbm", "xbn", "xbp", "xbq", "xbr", "xbs", "xbt", "xbw", "xbx")), hashes);
  }

  @Test
  public void testSimple() throws Exception {
    Set<String> hashes = GeoHashFinder.getNeighborsInDistance(0D, 0D, 100D);
    assertEquals(new TreeSet<>(Arrays.asList("7zz", "ebp", "kpb", "s00")), hashes);
  }
}