package org.apache.pinot.core.query.explainplan;

import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.query.request.context.QueryContext;

/**
 * The operator tree created by the plan maker may not be descriptive enough for Explain Plan output.
 * This builder uses query context to append missing information to the operator tree later
 * and prepares Explain Plan output.
 */
public class OperatorTreeToExplainPlanTreeBuilder {

  public OperatorTreeToExplainPlanTreeBuilder() {}

  public void buildExplainTree(QueryContext queryContext, Operator operator) {
    if (operator == null) {
      return;
    }
    if (operator instanceof ExpressionFilterOperator) {
      // add transform node
    }
  }
}

