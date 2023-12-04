package edu.colorado.cires.wod.iquodqc.check.icdcaqc01.levelorder;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.common.icdc.DepthData;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.Test;

class DepthDataTest {

  private final Path dataPath = Paths.get("src/test/resources/test-files/data.txt");


  /*
  def test_ICDC_level_order_simple(self):
        '''Simple tests to ensure functionality works as expected.
        '''
        p = util.testingProfile.fakeProfile([1.0, 2.0, 3.0],
                                            [2.0, -1.0, 1.0],
                                            uid=8888)
        ICDC.prepare_data_store(self.data_store)
        qc              = ICDC.test(p, self.parameters, self.data_store)
        nlevels, zr, tr = ICDC.reordered_data(p, self.data_store)
        zreverted       = ICDC.revert_order(p, zr, self.data_store)
        zreverted_truth = np.ma.array([2.0, -1.0, 1.0],
                                      mask = [False, True, False])

        assert np.array_equal(qc, [False, True, False]), 'QC error'
        assert nlevels == 2, 'Subsetting of levels incorrect'
        assert np.array_equal(zr, [1.0, 2.0]), 'Depth reorder failed'
        assert np.array_equal(tr, [3.0, 1.0]), 'Temperature reorder failed'
        assert np.array_equal(zreverted.data[zreverted.mask == False],
                              zreverted_truth.data[zreverted_truth.mask == False]),           'Revert data failed'
        assert np.array_equal(zreverted.mask,
                              zreverted_truth.mask), 'Revert data failed'
   */
  @Test
  public void testLevelOrderSimple() {
    int castNumber = 888;
    Cast cast = Cast.builder()
        .withCastNumber(castNumber)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 6)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(2.0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.0).build()))
                .build(),
            Depth.builder().withDepth(-1.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(2.0).build())).build(),
            Depth.builder().withDepth(1.0).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.0).build()))
                .build()
        ))
        .build();

    DepthData depthData = new DepthData(cast);
    assertEquals(castNumber, depthData.getUid());
    assertEquals(2, depthData.getnLevels());
    assertEquals(Arrays.asList(2, 0), depthData.getOriglevels());
    assertEquals(Arrays.asList(1.0, 2.0), depthData.getDepths());
    assertEquals(Arrays.asList(3.0, 1.0), depthData.getTemperatures());
    assertTrue(CollectionUtils.isEqualCollection(Arrays.asList(1), depthData.getAqc01Failures()));
  }

  /*
  def test_ICDC_level_order(self):
      '''
  Make sure code processes data supplied by Viktor Gouretski
  correctly.
        '''

  examples = [example1, example2, example3, example4, example5]

      for i, example in enumerate(examples):
      # Extract the test data and the 'truth' values.
      zorig   = example[:, 0]
  torig   = example[:, 1]
  ztruth  = example[:, 2]
  ttruth  = example[:, 3]
  qctruth = zorig < 0.0
  use     = ztruth >= 0.0
  ztruth  = ztruth[use]
  ttruth  = ttruth[use]
  p = util.testingProfile.fakeProfile(torig, zorig, uid=i)

      # Check the QC results are returned correctly.
      ICDC.prepare_data_store(self.data_store)
  qc = ICDC.test(p, self.parameters, self.data_store)
      assert np.array_equal(qc, qctruth), 'Example {} QC wrong'.format(i + 1)

            # Check that the reordering is correct.
  nlevels, zr, tr = ICDC.reordered_data(p, self.data_store)
      assert np.array_equal(zr, ztruth), 'Example {} zr wrong'.format(i + 1)
            assert np.array_equal(tr, ttruth), 'Example {} tr wrong'.format(i + 1)
   */
  @Test
  public void testLevelOrderExamples() throws Exception {
    List<Aqc02Data> examples = getTestData();
    for (Aqc02Data example: examples){
      Cast cast = example.buildCast();
      System.out.printf("testing %s\n", example.getHeader());
      DepthData depthData = new DepthData(cast);
      assertEquals(example.getReorderedDepth().size(), depthData.getnLevels());
      assertEquals(example.getReorderedDepth(), depthData.getDepths());
      assertEquals(example.getReorderedTemp(), depthData.getTemperatures());
      assertTrue(CollectionUtils.isEqualCollection(example.getQc(), depthData.getAqc01Failures()));
    }
  }

  private List<Aqc02Data> getTestData() throws Exception {
    List<Aqc02Data> examples = new ArrayList<>();
    BufferedReader reader;
    Aqc02Data example = null;
    try {
      reader = Files.newBufferedReader(dataPath);
      String line = reader.readLine();
      while (line != null) {
        if (line.substring(0, 7).equals("example")) {
          if (example != null) {
            examples.add(example);
          }
          example = new Aqc02Data(line);
        } else if (line.substring(0,1).equals("[")){
          example.addrow(line);
        }
        line = reader.readLine();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    examples.add(example);
    return examples;
  }
}