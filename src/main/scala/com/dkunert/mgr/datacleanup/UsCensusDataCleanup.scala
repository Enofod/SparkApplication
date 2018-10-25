package com.dkunert.mgr.datacleanup

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

object UsCensusDataCleanup {

  def cleanupData(inputData: DataFrame): DataFrame = {
    /*var dataset = inputData.select(
      inputData("dAge").cast("float"),
      inputData("dAncstry1").cast("float"),
      inputData("dAncstry2").cast("float"),
      inputData("iAvail").cast("float"),
      inputData("iCitizen").cast("float"),
      inputData("iClass").cast("float"),
      inputData("dDepart").cast("float"),
      inputData("iDisabl1").cast("float"),
      inputData("iDisabl2").cast("float"),
      inputData("iEnglish").cast("float"),
      inputData("iFeb55").cast("float"),
      inputData("iFertil").cast("float"),
      inputData("dHispanic").cast("float"),
      inputData("dHour89").cast("float"),
      inputData("dHours").cast("float"),
      inputData("iImmigr").cast("float"),
      inputData("dIncome1").cast("float"),
      inputData("dIncome2").cast("float"),
      inputData("dIncome3").cast("float"),
      inputData("dIncome4").cast("float"),
      inputData("dIncome5").cast("float"),
      inputData("dIncome6").cast("float"),
      inputData("dIncome7").cast("float"),
      inputData("dIncome8").cast("float"),
      inputData("dIndustry").cast("float"),
      inputData("iKorean").cast("float"),
      inputData("iLang1").cast("float"),
      inputData("iLooking").cast("float"),
      inputData("iMarital").cast("float"),
      inputData("iMay75880").cast("float"),
      inputData("iMeans").cast("float"),
      inputData("iMilitary").cast("float"),
      inputData("iMobility").cast("float"),
      inputData("iMobillim").cast("float"),
      inputData("dOccup").cast("float"),
      inputData("iOthrserv").cast("float"),
      inputData("iPerscare").cast("float"),
      inputData("dPOB").cast("float"),
      inputData("dPoverty").cast("float"),
      inputData("dPwgt1").cast("float"),
      inputData("iRagechld").cast("float"),
      inputData("dRearning").cast("float"),
      inputData("iRelat1").cast("float"),
      inputData("iRelat2").cast("float"),
      inputData("iRemplpar").cast("float"),
      inputData("iRiders").cast("float"),
      inputData("iRlabor").cast("float"),
      inputData("iRownchld").cast("float"),
      inputData("dRpincome").cast("float"),
      inputData("iRPOB").cast("float"),
      inputData("iRrelchld").cast("float"),
      inputData("iRspouse").cast("float"),
      inputData("iRvetserv").cast("float"),
      inputData("iSchool").cast("float"),
      inputData("iSept80").cast("float"),
      inputData("iSex").cast("float"),
      inputData("iSubfam1").cast("float"),
      inputData("iSubfam2").cast("float"),
      inputData("iTmpabsnt").cast("float"),
      inputData("dTravtime").cast("float"),
      inputData("iVietnam").cast("float"),
      inputData("dWeek89").cast("float"),
      inputData("iWork89").cast("float"),
      inputData("iWorklwk").cast("float"),
      inputData("iWWII").cast("float"),
      inputData("iYearsch").cast("float"),
      inputData("iYearwrk").cast("float"),
      inputData("dYrsserv").cast("float")
    )*/

    var dataset = inputData.select(
      inputData("dAge").cast("float"),
      inputData("iSex").cast("float"),
      inputData("iCitizen").cast("float"),
      inputData("dDepart").cast("float"),
      inputData("iImmigr").cast("float"),
      inputData("iSchool").cast("float"),
      inputData("iEnglish").cast("float"),
      inputData("dHispanic").cast("float"),
      inputData("iKorean").cast("float"),
      inputData("iVietnam").cast("float")
    )

    dataset = dataset.na.drop()

    //val requiredFeatures = Array("dAge", "dAncstry1", "dAncstry2", "iAvail", "iCitizen", "iClass", "dDepart", "iDisabl1", "iDisabl2", "iEnglish", "iFeb55", "iFertil", "dHispanic", "dHour89", "dHours", "iImmigr", "dIncome1", "dIncome2", "dIncome3", "dIncome4", "dIncome5", "dIncome6", "dIncome7", "dIncome8", "dIndustry", "iKorean", "iLang1", "iLooking", "iMarital", "iMay75880", "iMeans", "iMilitary", "iMobility", "iMobillim", "dOccup", "iOthrserv", "iPerscare", "dPOB", "dPoverty", "dPwgt1", "iRagechld", "dRearning", "iRelat1", "iRelat2", "iRemplpar", "iRiders", "iRlabor", "iRownchld", "dRpincome", "iRPOB", "iRrelchld", "iRspouse", "iRvetserv", "iSchool", "iSept80", "iSex", "iSubfam1", "iSubfam2", "iTmpabsnt", "dTravtime", "iVietnam", "dWeek89", "iWork89", "iWorklwk", "iWWII", "iYearsch", "iYearwrk", "dYrsserv")
    val requiredFeatures = Array("dAge", "iSex", "iCitizen", "dDepart", "iImmigr", "iSchool", "iEnglish", "dHispanic", "iKorean", "iVietnam")

    val assembler = new VectorAssembler()
      .setInputCols(requiredFeatures)
      .setOutputCol("features")

    return assembler.transform(dataset)
  }
}
