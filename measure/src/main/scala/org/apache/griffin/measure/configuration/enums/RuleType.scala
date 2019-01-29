package org.apache.griffin.measure.configuration.enums

import scala.util.matching.Regex

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2019/1/28
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2019/1/28     1.0.1.0       tang.peng     创建
  * ----------------------------------------------------------------
  */
sealed trait RuleType {
  val regex: Regex
  val desc: String
}

object RuleType {
  private val ruleTypes: List[RuleType] = List(
    RecordCountType,CheckType,AggregationType,ZipperType,VolabilityType,MutiLineType, UnknownRuleType
  )

  def apply(ptn: String): RuleType = {

    ruleTypes.filter(tp => ptn match {
      case tp.regex() => true
      case _ => false
    }).headOption.getOrElse(UnknownRuleType)
  }

  def unapply(pt: RuleType): Option[String] = Some(pt.desc)
}

final case object RecordCountType extends RuleType {
  val regex = "^(?i)record".r
  val desc = "record"
}

final case object CheckType extends RuleType {
  val regex = "^(?i)check".r
  val desc = "check"
}
final case object AggregationType extends RuleType {
  val regex = "aggr".r
  val desc = "aggr"
}
final case object ZipperType extends RuleType {
  val regex = "zipper".r
  val desc = "zipper"
}
final case object VolabilityType extends RuleType {
  val regex = "volability".r
  val desc = "volability"
}
final case object MutiLineType extends RuleType {
  val regex = "muti".r
  val desc = "mutiline"
}

final case object UnknownRuleType extends RuleType {
  val regex = "".r
  val desc = "unknown"
}



