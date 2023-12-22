package edu.colorado.cires.wod.iquodqc.check.codete.woanormbias;


import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.BaseCoTeDeWoaNormbiasCheck;

public class CoTeDeGtsppWoaNormbiasCheck extends BaseCoTeDeWoaNormbiasCheck {


  public CoTeDeGtsppWoaNormbiasCheck() {
    super(3D);
  }

  @Override
  public String getName() {
    return CheckNames.COTEDE_GTSPP_WOA_NORMBIAS.getName();
  }


}
