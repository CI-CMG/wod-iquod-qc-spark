package edu.colorado.cires.wod.iquodqc.check.codete.woanormbias;


import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.BaseCoTeDeWoaNormbiasCheck;

public class CoTeDeWoaNormbiasCheck extends BaseCoTeDeWoaNormbiasCheck {


  public CoTeDeWoaNormbiasCheck() {
    super(10D);
  }

  @Override
  public String getName() {
    return CheckNames.COTEDE_WOA_NORMBIAS.getName();
  }


}
