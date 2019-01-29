package org.apache.griffin.measure.step.builder.dsl.transform.analyzer

import org.apache.griffin.measure.step.builder.dsl.expr.{AliasableExpr, LogicalExpr, SelectionExpr}

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2018-07-31
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-07-31     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
case class ValidityAnalyzer (expr: LogicalExpr, sourceName: String) extends BasicAnalyzer{
  val dataSourceNames = expr.preOrderTraverseDepthFirst(Set[String]())(seqDataSourceNames, combDataSourceNames)

  val sourceSelectionExprs = {
    val seq = seqSelectionExprs(sourceName)
    expr.preOrderTraverseDepthFirst(Seq[SelectionExpr]())(seq, combSelectionExprs)
  }

  val selectionExprs = sourceSelectionExprs ++ {
    expr.preOrderTraverseDepthFirst(Seq[AliasableExpr]())(seqWithAliasExprs, combWithAliasExprs)
  }
}
