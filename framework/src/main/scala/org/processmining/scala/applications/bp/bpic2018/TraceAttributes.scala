package org.processmining.scala.applications.bp.bpic2018

import org.processmining.scala.log.common.xes.parallel.XesReader

class BaseTraceAttributes(prefix: String) extends Serializable {
  val Application = s"${prefix}application"
  val Applicant = s"${prefix}applicant"
  val CrossCompliance = s"${prefix}cross_compliance"
  val Department = s"${prefix}department"
  val Rejected = s"${prefix}rejected"
  val Year = s"${prefix}year"

  val SetOfOthers = Set(Applicant, Application, CrossCompliance, Department, Rejected, Year)

  val TrAttrNumberParcels = s"${prefix}number_parcels"
  val TrAttrArea = s"${prefix}area"
  val TrAttrRedistribution = s"${prefix}redistribution"
  val TrAttrSmallFarmer = s"${prefix}small farmer"
  val TrAttrYoungFarmer = s"${prefix}young farmer"
  val SetOfTypeOfParcels = Set(TrAttrNumberParcels, TrAttrArea, TrAttrRedistribution, TrAttrSmallFarmer, TrAttrYoungFarmer)

  val TrAttrAmountApplied0 = s"${prefix}amount_applied0"
  val TrAttrAmountApplied1 = s"${prefix}amount_applied1"
  val TrAttrAmountApplied2 = s"${prefix}amount_applied2"
  val TrAttrAmountApplied3 = s"${prefix}amount_applied3"
  val TrAttrPaymentActual0 = s"${prefix}payment_actual0"
  val TrAttrPaymentActual1 = s"${prefix}payment_actual1"
  val TrAttrPaymentActual2 = s"${prefix}payment_actual2"
  val TrAttrPaymentActual3 = s"${prefix}payment_actual3"
  val TrAttrPenaltyAmount0 = s"${prefix}penalty_amount0"
  val TrAttrPenaltyAmount1 = s"${prefix}penalty_amount1"
  val TrAttrPenaltyAmount2 = s"${prefix}penalty_amount2"
  val TrAttrPenaltyAmount3 = s"${prefix}penalty_amount3"
  val SetOfPayments = Set(TrAttrAmountApplied0, TrAttrAmountApplied1, TrAttrAmountApplied2, TrAttrAmountApplied3, TrAttrPaymentActual0, TrAttrPaymentActual1, TrAttrPaymentActual2, TrAttrPaymentActual3, TrAttrPenaltyAmount0, TrAttrPenaltyAmount1, TrAttrPenaltyAmount2, TrAttrPenaltyAmount3)

  val TrAttrPenaltyB16 = s"${prefix}penalty_B16"
  val TrAttrPenaltyB3 = s"${prefix}penalty_B3"
  val TrAttrPenaltyB4 = s"${prefix}penalty_B4"
  val TrAttrPenaltyB5 = s"${prefix}penalty_B5"
  val TrAttrPenaltyB6 = s"${prefix}penalty_B6"
  val TrAttrPenaltyBG = s"${prefix}penalty_BGK"
  val TrAttrPenaltyC16 = s"${prefix}penalty_C16"
  val TrAttrPenaltyJLP3 = s"${prefix}penalty_JLP3"
  val TrAttrPenaltyV5 = s"${prefix}penalty_V5"
  val SetOfSeverePenalties = Set(TrAttrPenaltyB16, TrAttrPenaltyB3, TrAttrPenaltyB4, TrAttrPenaltyB5, TrAttrPenaltyB6, TrAttrPenaltyBG, TrAttrPenaltyC16, TrAttrPenaltyJLP3, TrAttrPenaltyV5)

  val TrAttrPenaltyABP = s"${prefix}penalty_ABP"
  val TrAttrPenaltyAGP = s"${prefix}penalty_AGP"
  val TrAttrPenaltyAJLP = s"${prefix}penalty_AJLP"
  val TrAttrPenaltyAUVP = s"${prefix}penalty_AUVP"
  val TrAttrPenaltyAVBP = s"${prefix}penalty_AVBP"
  val TrAttrPenaltyAVGP = s"${prefix}penalty_AVGP"
  val TrAttrPenaltyAVJLP = s"${prefix}penalty_AVJLP"
  val TrAttrPenaltyAVUVP = s"${prefix}penalty_AVUVP"
  val TrAttrPenaltyB2 = s"${prefix}penalty_B2"
  val TrAttrPenaltyB5F = s"${prefix}penalty_B5F"
  val TrAttrPenaltyBGKV = s"${prefix}penalty_BGKV"
  val TrAttrPenaltyBGP = s"${prefix}penalty_BGP"
  val TrAttrPenaltyC4 = s"${prefix}penalty_C4"
  val TrAttrPenaltyC9 = s"${prefix}penalty_C9"
  val TrAttrPenaltyCC = s"${prefix}penalty_CC"
  val TrAttrPenaltyGP1 = s"${prefix}penalty_GP1"
  val TrAttrPenaltyJLP1 = s"${prefix}penalty_JLP1"
  val TrAttrPenaltyJLP2 = s"${prefix}penalty_JLP2"
  val TrAttrPenaltyJLP5 = s"${prefix}penalty_JLP5"
  val TrAttrPenaltyJLP6 = s"${prefix}penalty_JLP6"
  val TrAttrPenaltyJLP7 = s"${prefix}penalty_JLP7"
  val SetOfPenalties = Set(TrAttrPenaltyABP, TrAttrPenaltyAGP, TrAttrPenaltyAJLP, TrAttrPenaltyAUVP, TrAttrPenaltyAVBP, TrAttrPenaltyAVGP, TrAttrPenaltyAVJLP, TrAttrPenaltyAVUVP, TrAttrPenaltyB2, TrAttrPenaltyB5F, TrAttrPenaltyBGKV, TrAttrPenaltyBGP, TrAttrPenaltyC4, TrAttrPenaltyC9, TrAttrPenaltyCC, TrAttrPenaltyGP1, TrAttrPenaltyJLP1, TrAttrPenaltyJLP2, TrAttrPenaltyJLP5, TrAttrPenaltyJLP6, TrAttrPenaltyJLP7)



  val TrAttrSelectedRisk = s"${prefix}selected_risk"
  val TrAttrSelectedManually = s"${prefix}selected_manually"
  val TrAttrSelectedRandom = s"${prefix}selected_random"
  val SetOfRisks = Set(TrAttrSelectedRisk, TrAttrSelectedManually, TrAttrSelectedRandom)

  val SetOfMainAttributes =  SetOfOthers ++ SetOfPayments ++ SetOfPenalties ++ SetOfSeverePenalties ++ SetOfRisks ++ SetOfTypeOfParcels
  val SetOfMinAttributes = Set(Application, Year)
}

object TraceAttributes extends BaseTraceAttributes(XesReader.DefaultTraceAttrNamePrefix)

object DiscoTraceAttributes extends BaseTraceAttributes(XesReader.DiscoTraceAttrNamePrefix)

object TraceAttributesWithoutPrefix extends BaseTraceAttributes("")
