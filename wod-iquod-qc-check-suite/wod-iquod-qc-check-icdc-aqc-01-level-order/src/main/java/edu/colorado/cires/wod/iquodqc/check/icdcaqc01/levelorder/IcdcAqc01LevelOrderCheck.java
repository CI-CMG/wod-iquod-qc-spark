package edu.colorado.cires.wod.iquodqc.check.icdcaqc01.levelorder;

import static edu.colorado.cires.wod.iquodqc.common.CheckNames.ICDC_AQC_01_LEVEL_ORDER;

import edu.colorado.cires.wod.iquodqc.check.api.CommonCastCheck;
import edu.colorado.cires.wod.iquodqc.common.icdc.DepthData;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;

public class IcdcAqc01LevelOrderCheck extends CommonCastCheck {

  @Override
  public String getName() {
    return ICDC_AQC_01_LEVEL_ORDER.getName();
  }
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
  @Override
  protected Collection<Integer> getFailedDepths(Cast cast) {
    DepthData depthData = new DepthData(cast);
    return depthData.getAqc01Failures();
  }

}
