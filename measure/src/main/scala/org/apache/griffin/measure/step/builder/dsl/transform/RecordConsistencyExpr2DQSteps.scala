package org.apache.griffin.measure.step.builder.dsl.transform

import org.apache.griffin.measure.configuration.dqdefinition.RuleParam
import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.step.DQStep
import org.apache.griffin.measure.step.builder.dsl.expr.{Expr, LogicalExpr}
import org.apache.griffin.measure.step.builder.dsl.transform.analyzer.AccuracyAnalyzer
import org.apache.griffin.measure.step.transform.SparkSqlTransformStep
import org.apache.griffin.measure.step.write.MetricWriteStep
import org.apache.griffin.measure.utils.ParamUtil._


/**
  *
  * 模块名称：记录一致性检查
  * 模块描述：两表之间记录数是否相等
  * 开发作者：tang.peng
  * 创建日期：2018-07-25
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2018-07-25     1.0.1.0    tang.peng        创建
  * ----------------------------------------------------------------
  */
case class RecordConsistencyExpr2DQSteps(context: DQContext,
                                         expr: Expr,
                                         ruleParam: RuleParam
                                   ) extends Expr2DQSteps {

  private object RecordCountKeys {
    val _source = "source"
    val _target = "target"
    val _source_count = "source.count"
    val _target_count = "target.count"
  }

  import RecordCountKeys._

  override def getDQSteps(): Seq[DQStep] = {
    val details = ruleParam.getDetails
    val recordCountExpr = expr.asInstanceOf[LogicalExpr]
    val sourceName = details.getString(_source, context.getDataSourceName(0))
    val targetName = details.getString(_target, context.getDataSourceName(1))
    val newAnalyzer = AccuracyAnalyzer(recordCountExpr, sourceName, targetName)
    val procType = context.procType
    val timestamp = context.contextId.timestamp

    if (!context.runTimeTableRegister.existsTable(sourceName)) {
      warn(s"[${timestamp}] data source ${sourceName} not exists")
      Nil
    } else {
      val sourceCount = details.getString(_source_count, "SOURCE_COUNT")
      val targetCount = details.getString(_target_count, "TARGET_COUNT")

      val sourceSelectClause = newAnalyzer.sourceSelectionExprs.map { sel =>
        s"COUNT(${sel.desc}) AS ${sourceCount} , 'COUNT' AS KEY "
      }.head
      val targetSelectClause = newAnalyzer.targetSelectionExprs.map { sel =>
        s"COUNT(${sel.desc}) AS ${targetCount}, 'COUNT' AS KEY "
      }.head
      val onClause = "`C1`.`KEY` = `C2`.`KEY`"
      val recordMetricSql =  s"""
         | SELECT
         |   `C1`.`${sourceCount}`,
         |   `C2`.`${targetCount}`,
         |   CASE WHEN `C1`.`${sourceCount}` = `C2`.`${targetCount}` THEN true ELSE false END AS RESULT
         | FROM
         | (SELECT ${sourceSelectClause} FROM `${sourceName}` ) `C1`
         | INNER JOIN
         | (SELECT ${targetSelectClause}  FROM `${targetName}` ) `C2`
         | ON ${onClause}
      """.stripMargin

      val recordCountTableName = ruleParam.getOutDfName()
      val recordCountTransStep = SparkSqlTransformStep(recordCountTableName, recordMetricSql, emptyMap)


      val accuracyMetricWriteSteps = procType match {
        case BatchProcessType =>
          val metricOpt = ruleParam.getOutputOpt(MetricOutputType)
          val mwName = metricOpt.flatMap(_.getNameOpt).getOrElse(ruleParam.getOutDfName())
          val flattenType = metricOpt.map(_.getFlatten).getOrElse(FlattenType.default)
          MetricWriteStep(mwName, recordCountTableName, flattenType) :: Nil
        case StreamingProcessType => Nil
      }
      val transSteps1 = recordCountTransStep :: Nil

      val writeSteps1 = accuracyMetricWriteSteps
      transSteps1 ++ writeSteps1

    }
  }

}
