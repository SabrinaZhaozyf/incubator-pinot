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

package org.apache.pinot.core.query.aggregation.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.CovarianceTuple;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Aggregation function which returns the population covariance of 2 expressions.
 * cov_pop(exp1, exp2) = mean(exp1 * exp2) - mean(exp1) * mean(exp2)
 *
 */
public class CovarianceAggregationFunction implements AggregationFunction<CovarianceTuple, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;
  protected final ExpressionContext _expression1;
  protected final ExpressionContext _expression2;

  public CovarianceAggregationFunction(List<ExpressionContext> arguments) {
    _expression1 = arguments.get(0);
    _expression2 = arguments.get(1);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.COVPOP;
  }

  @Override
  public String getColumnName() {
    return getType().getName() + "_" + _expression1 + "_" + _expression2;
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression1 + "," + _expression2 + ")";
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    ArrayList<ExpressionContext> inputExpressions = new ArrayList<>();
    inputExpressions.add(_expression1);
    inputExpressions.add(_expression2);
    return inputExpressions;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet1 = blockValSetMap.get(_expression1);
    BlockValSet blockValSet2 = blockValSetMap.get(_expression2);
    double[] values1;
    double[] values2;
    switch (blockValSet1.getValueType().getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        values1 = blockValSet1.getDoubleValuesSV();
        break;
      default:
        throw new IllegalStateException(
            "Cannot compute covariance for non-numeric type: " + blockValSet1.getValueType());
    }

    switch (blockValSet2.getValueType().getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        values2 = blockValSet2.getDoubleValuesSV();
        break;
      default:
        throw new IllegalStateException(
            "Cannot compute covariance for non-numeric type: " + blockValSet2.getValueType());
    }

    double sumX = 0.0;
    double sumY = 0.0;
    double sumXY = 0.0;

    for (int i = 0; i < length; i++) {
      sumX += values1[i];
      sumY += values2[i];
      sumXY += values1[i] * values2[i];
    }
    setAggregationResult(aggregationResultHolder, sumX, sumY, sumXY, length);
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double sumX, double sumY,
      double sumXY, long count) {
    CovarianceTuple covarianceTuple = aggregationResultHolder.getResult();
    if (covarianceTuple == null) {
      aggregationResultHolder.setValue(new CovarianceTuple(sumX, sumY, sumXY, count));
    } else {
      covarianceTuple.apply(sumX, sumY, sumXY, count);
    }
  }

  protected void setGroupByResult() {}


  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    // TODO
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    // TODO
  }

  @Override
  public CovarianceTuple extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    CovarianceTuple covarianceTuple = aggregationResultHolder.getResult();
    if (covarianceTuple == null) {
      return new CovarianceTuple(0.0, 0.0, 0.0, 0L);
    } else {
      return covarianceTuple;
    }
  }

  @Override
  public CovarianceTuple extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return null;
  }

  @Override
  public CovarianceTuple merge(CovarianceTuple intermediateResult1, CovarianceTuple intermediateResult2) {
    intermediateResult1.apply(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(CovarianceTuple covarianceTuple) {
    long count = covarianceTuple.getCount();
    if (count == 0L) {
      return DEFAULT_FINAL_RESULT;
    } else {
      double sumX = covarianceTuple.getSumX();
      double sumY = covarianceTuple.getSumY();
      double sumXY = covarianceTuple.getSumXY();
      return (sumXY / count) - (sumX / count) * (sumY / count);
    }
  }

  @Override
  public String toExplainString() {
    // TODO
    return null;
  }
}
