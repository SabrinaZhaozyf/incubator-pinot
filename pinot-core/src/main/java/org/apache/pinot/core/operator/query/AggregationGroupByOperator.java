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
package org.apache.pinot.core.operator.query;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByExecutor;
import org.apache.pinot.core.startree.executor.StarTreeGroupByExecutor;


/**
 * The <code>AggregationGroupByOperator</code> class provides the operator for aggregation group-by query on a single
 * segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationGroupByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "AggregationGroupByOperator";

  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final TransformOperator _transformOperator;
  private final long _numTotalDocs;
  private final boolean _useStarTree;

  private int _numDocsScanned = 0;

  public AggregationGroupByOperator(AggregationFunction[] aggregationFunctions, ExpressionContext[] groupByExpressions,
      int maxInitialResultHolderCapacity, int numGroupsLimit, TransformOperator transformOperator, long numTotalDocs,
      boolean useStarTree) {
    _aggregationFunctions = aggregationFunctions;
    _groupByExpressions = groupByExpressions;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _transformOperator = transformOperator;
    _numTotalDocs = numTotalDocs;
    _useStarTree = useStarTree;
    _childOperators.add(transformOperator);
    _explainPlanName = "AGGREGATE_GROUPBY";
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    // Perform aggregation group-by on all the blocks
    GroupByExecutor groupByExecutor;
    if (_useStarTree) {
      groupByExecutor =
          new StarTreeGroupByExecutor(_aggregationFunctions, _groupByExpressions, _maxInitialResultHolderCapacity,
              _numGroupsLimit, _transformOperator);
    } else {
      groupByExecutor =
          new DefaultGroupByExecutor(_aggregationFunctions, _groupByExpressions, _maxInitialResultHolderCapacity,
              _numGroupsLimit, _transformOperator);
    }
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      _numDocsScanned += transformBlock.getNumDocs();
      groupByExecutor.process(transformBlock);
    }

    // Build intermediate result block based on aggregation group-by result from the executor
    return new IntermediateResultsBlock(_aggregationFunctions, groupByExecutor.getResult());
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _transformOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String getOperatorDetails() {
    StringBuilder stringBuilder = new StringBuilder(_explainPlanName).append("(groupKeys:");
    for (int i = 0; i < _groupByExpressions.length; i++) {
      stringBuilder.append(_groupByExpressions[i].toString()).append(',');
    }

    stringBuilder.append("aggregations:");
    int count = 0;
    for (AggregationFunction func : _aggregationFunctions) {
      if (count == _aggregationFunctions.length - 1) {
        stringBuilder.append(func.toString());
      } else {
        stringBuilder.append(func.toString()).append(", ");
      }
      count++;
    }
    return stringBuilder.append(')').toString();
  }
}
