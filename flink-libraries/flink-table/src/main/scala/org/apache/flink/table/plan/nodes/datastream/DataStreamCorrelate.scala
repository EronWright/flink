/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.sql.SemiJoinType
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.FlinkCorrelate
import org.apache.flink.table.typeutils.TypeConverter._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamTableEnvironment

/**
  * Flink RelNode which matches along with join a user defined table function.
  */
class DataStreamCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    scan: LogicalTableFunctionScan,
    condition: Option[RexNode],
    relRowType: RelDataType,
    joinRowType: RelDataType,
    joinType: SemiJoinType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, inputNode)
  with FlinkCorrelate
  with DataStreamRel {

  override def deriveRowType() = relRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamCorrelate(
      cluster,
      traitSet,
      inputs.get(0),
      scan,
      condition,
      relRowType,
      joinRowType,
      joinType,
      ruleDescription)
  }

  override def toString: String = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    correlateToString(rexCall, sqlFunction)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    super.explainTerms(pw)
      .item("invocation", scan.getCall)
      .item("function", sqlFunction.getTableFunction.getClass.getCanonicalName)
      .item("rowType", relRowType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      expectedType: Option[TypeInformation[Any]])
    : DataStream[Any] = {

    val config = tableEnv.getConfig
    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    // we do not need to specify input type
    val inputDS = inputNode.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    val funcRel = scan.asInstanceOf[LogicalTableFunctionScan]
    val rexCall = funcRel.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    val pojoFieldMapping = sqlFunction.getPojoFieldMapping
    val udtfTypeInfo = sqlFunction.getRowTypeInfo.asInstanceOf[TypeInformation[Any]]

    val generator = new CodeGenerator(
      config,
      false,
      inputDS.getType,
      Some(udtfTypeInfo),
      None,
      Some(pojoFieldMapping))

    val body = functionBody(
      generator,
      udtfTypeInfo,
      getRowType,
      rexCall,
      condition,
      config,
      joinType,
      expectedType)

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Any, Any]],
      body,
      returnType)

    val mapFunc = correlateMapFunction(genFunction)

    inputDS.flatMap(mapFunc).name(correlateOpName(rexCall, sqlFunction, relRowType))
  }

}
