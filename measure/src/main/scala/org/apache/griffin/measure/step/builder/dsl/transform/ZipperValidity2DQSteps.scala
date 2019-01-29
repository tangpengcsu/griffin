package org.apache.griffin.measure.step.builder.dsl.transform

import org.apache.griffin.measure.configuration.dqdefinition.RuleParam
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.step.DQStep
import org.apache.griffin.measure.step.builder.dsl.expr.Expr

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2019/1/29
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2019/1/29     1.0.1.0       tang.peng     创建
  * ----------------------------------------------------------------
  */
case class ZipperValidity2DQSteps (context: DQContext,
                              expr: Expr,
                              ruleParam: RuleParam
                             ) extends Expr2DQSteps{
  override def getDQSteps(): Seq[DQStep] = ???
}
