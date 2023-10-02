package edu.colorado.cires.wod.iquodqc.common.icdc;

import edu.colorado.cires.wod.iquodqc.common.DepthUtils;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DepthData {
  private int uid;

  private int castNLevels;
  private int nLevels;
  private List<Integer> origlevels = new ArrayList<>();
  private List<Double> depths = new ArrayList<>();
  private List<Double> temperatures = new ArrayList<>();

  private Set<Integer> aqc01Failures = new LinkedHashSet<>();

  /*
  def level_order(p, data_store):
    '''Reorders data into depth order and rejects levels with
       negative depth.
    '''

    # check if the relevant info is already in the db
    precomputed = data_store.get(p.uid(), 'icdclevelorder')
    if precomputed:
        return p.uid(), precomputed['nlevels'], precomputed['origlevels'], precomputed['zr'], precomputed['tr'], precomputed['qc']

    # Extract data and define the index for each level.
    z          = p.z()
    t          = p.t()
    origlevels = np.arange(p.n_levels())

    # Implement the QC. For this test we only reject negative depths.
    qc = z < 0

    # Remove occurrences of no data at a level and rejected obs.
    use        = (z.mask == False) & (t.mask == False) & (qc == False)
    z          = z[use]
    t          = t[use]
    origlevels = origlevels[use]
    nlevels    = np.count_nonzero(use)

    if nlevels > 1:
        # Sort the data. Using mergesort keeps levels with the same depth
        # in the same order.
        isort      = np.argsort(z, kind='mergesort')
        zr         = z[isort]
        tr         = t[isort]
        origlevels = origlevels[isort]
    else:
        zr         = z
        tr         = t

    # register pre-computed arrays in db for reuse
    data_store.put(p.uid(), 'icdclevelorder', {'nlevels':nlevels, 'origlevels':origlevels, 'zr':zr, 'tr':tr, 'qc':qc})

    return p.uid(), nlevels, origlevels, zr, tr, qc

   */
  public DepthData(Cast cast) {
    List<Depth> depths = cast.getDepths();
    this.uid = cast.getCastNumber();
    this.castNLevels = depths.size();

    for (int i = 0; i < depths.size(); i++) {
      Depth depth = depths.get(i);
      if (depth.getDepth() < 0D){
        this.aqc01Failures.add(i);
      } else {
        Optional<ProfileData> temperature = DepthUtils.getTemperature(depth);
        if (temperature.isPresent()){
          sort(i, depth.getDepth(), temperature.get().getValue());
        }
      }
    }
    this.nLevels = this.depths.size();
  }

  private void sort(int level, double depth, double temp){
    // add level, depth, temp to end of lists
    this.origlevels.add(level);
    this.depths.add(depth);
    this.temperatures.add(temp);

    // sort valuse
    int i = this.depths.size()-2;
    while (i > -1){
      if (depth < this.depths.get(i)){
        this.origlevels.set(i + 1, this.origlevels.get(i));
        this.origlevels.set(i, level);
        this.depths.set(i + 1, this.depths.get(i));
        this.depths.set(i, depth);
        this.temperatures.set(i + 1, this.temperatures.get(i));
        this.temperatures.set(i, temp);
      } else {
        break;
      }
      i -=1;
    }
  }
  public int getUid() {
    return uid;
  }

  public int getnLevels() {
    return nLevels;
  }

  public List<Integer> getOriglevels() {
    return origlevels;
  }

  public List<Double> getDepths() {
    return depths;
  }

  public List<Double> getTemperatures() {
    return temperatures;
  }

  public Set<Integer> getAqc01Failures() {
    return aqc01Failures;
  }

}
