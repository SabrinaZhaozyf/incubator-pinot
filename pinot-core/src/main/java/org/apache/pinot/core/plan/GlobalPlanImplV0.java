/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.plan;

import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.InstanceResponseOperator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.explain.ExplainPlanTreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GlobalPlan for a query applied to all the pruned segments.
 *
 *
 */
public class GlobalPlanImplV0 implements Plan {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPlanImplV0.class);

  private final InstanceResponsePlanNode _instanceResponsePlanNode;

  public GlobalPlanImplV0(InstanceResponsePlanNode instanceResponsePlanNode) {
    _instanceResponsePlanNode = instanceResponsePlanNode;
  }

  /** Builds a plan tree with node being the root.
   * Note that this function is only used for regular operators (not query rewrite) */
  private void buildPlan(Operator node, int indentation, StringBuilder sb) {
    if (node == null) {
      return;
    }
    if (!node.skipInExplainPlan()) {
      for (int i=0; i < indentation; i++){
        sb.append(" ");
      }
      sb.append(node.getOperatorDetails()).append("\n");
    }
    for (Object child : node.getChildOperators()) {
      buildPlan((Operator) child, indentation + 4, sb);
    }
  }

  public InstanceResponsePlanNode getInstanceResponsePlanNode() {
    return _instanceResponsePlanNode;
  }

  @Override
  public DataTable execute() {
    long startTime = System.currentTimeMillis();
    InstanceResponseOperator instanceResponseOperator = _instanceResponsePlanNode.run();
    Operator root = instanceResponseOperator.getChildOperators().get(0);
    StringBuilder sb = new StringBuilder();
    buildPlan(root, 0, sb);
    System.out.println(sb.toString());
    long endTime1 = System.currentTimeMillis();
    LOGGER.debug("InstanceResponsePlanNode.run() took: {}ms", endTime1 - startTime);
    InstanceResponseBlock instanceResponseBlock = instanceResponseOperator.nextBlock();
    long endTime2 = System.currentTimeMillis();
    LOGGER.debug("InstanceResponseOperator.nextBlock() took: {}ms", endTime2 - endTime1);
    return instanceResponseBlock.getInstanceResponseDataTable();
  }
}
