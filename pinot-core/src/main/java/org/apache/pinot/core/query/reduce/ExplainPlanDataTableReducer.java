package org.apache.pinot.core.query.reduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExplainPlanDataTableReducer implements DataTableReducer{
  private static final Logger LOGGER = LoggerFactory.getLogger(ExplainPlanDataTableReducer.class);

  private final QueryContext _queryContext;
  private final boolean _preserveType;
  private final boolean _responseFormatSql;

  ExplainPlanDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    QueryOptions queryOptions = new QueryOptions(queryContext.getQueryOptions());
    _preserveType = queryOptions.isPreserveType();
    _responseFormatSql = queryOptions.isResponseFormatSQL();
  }

  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    Map.Entry<ServerRoutingInstance, DataTable> entry = dataTableMap.entrySet().iterator().next();
    DataTable dataTable = entry.getValue();
    List<Object[]> reducedRows = new ArrayList<>();
    int[] idArray = new int[1];
    idArray[0] = 0;
    // add query rewrite operations
    addQueryRewriteToTable(reducedRows, idArray);
    // add broker reduce node
    addBrokerReduceOperationToTable(reducedRows, idArray);
    // add node starting from server combine
    int numAddedOps = idArray[0];
    int numRows = dataTable.getNumberOfRows();
    for (int rowId = 0; rowId < numRows; rowId++) {
      Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
      row[1] = (int) row[1] + numAddedOps;
      row[2] = (int) row[2] + numAddedOps;
      reducedRows.add(row);
    }
    ResultTable resultTable = new ResultTable(dataSchema, reducedRows);
    brokerResponseNative.setResultTable(resultTable);
  }

  private void addQueryRewriteToTable(List<Object[]> resultRows, int[] globalId) {
    Map<String, String> options;
    options = _queryContext.getQueryOptions() == null ? new HashMap<>() : _queryContext.getQueryOptions();
    if (!options.containsKey(CalciteSqlParser.QUERY_REWRITE)) {
      return;
    }
    // add the parent node for all query rewrite operations
    Object[] firstRow = new Object[3];
    firstRow[0] = CalciteSqlParser.QUERY_REWRITE;
    firstRow[1] = globalId[0];
    globalId[0] = globalId[0] + 1;
    firstRow[2] = -1;
    resultRows.add(firstRow);
    // add all possible query rewrite operations
    tryAddRewriteOperationToTable(options, resultRows, globalId, CalciteSqlParser.INVOKE_COMPILATION_TIME_FUNCTIONS);
    tryAddRewriteOperationToTable(options, resultRows, globalId, CalciteSqlParser.REWRITE_SELECTIONS);
    tryAddRewriteOperationToTable(options, resultRows, globalId, CalciteSqlParser.UPDATE_COMPARISON_PREDICATES);
    tryAddRewriteOperationToTable(options, resultRows, globalId, CalciteSqlParser.APPLY_ORDINALS);
    tryAddRewriteOperationToTable(options, resultRows, globalId, CalciteSqlParser.REWRITE_NON_AGGREGATION_GROUPBY_TO_DISTINCT);
    tryAddRewriteOperationToTable(options, resultRows, globalId, CalciteSqlParser.APPLY_ALIAS);
  }

  private void tryAddRewriteOperationToTable
      (Map<String, String> options, List<Object[]> resultRows, int[] globalId, String operationToAdd) {
    if (options.containsKey(operationToAdd)) {
      Object[] newRow = new Object[3];
      if (operationToAdd.equals(CalciteSqlParser.REWRITE_NON_AGGREGATION_GROUPBY_TO_DISTINCT)) {
        // no attributes for rewriting non-agg group-by to distinct
        newRow[0] = operationToAdd;
      } else {
        newRow[0] = operationToAdd + '(' + options.get(operationToAdd) + ')';
      }
      newRow[1] = globalId[0];
      globalId[0] = globalId[0] + 1;
      // parentId is always 0 for query rewrite operations
      newRow[2] = 0;
      resultRows.add(newRow);
    }
  }

  private void addBrokerReduceOperationToTable(List<Object[]> resultRows, int[] globalId) {
    String havingFilter = _queryContext.getHavingFilter() != null ? _queryContext.getHavingFilter().toString() : null;
    String sort = _queryContext.getOrderByExpressions() != null ? _queryContext.getOrderByExpressions().toString() : null;
    int limit = _queryContext.getLimit();
    if (QueryContextUtils.isAggregationQuery(_queryContext) && _queryContext.getGroupByExpressions() == null) {
      // queries with 1 or more aggregation only functions always returns at most 1 row
      limit = 1;
    }
    Set<String> postAggregations = new HashSet<>();
    Set<String> regularTransforms = new HashSet<>();
    QueryContextUtils.generateTransforms(_queryContext, postAggregations, regularTransforms);
    StringBuilder stringBuilder = new StringBuilder("BROKER_REDUCE").append('(');
    if (havingFilter != null) {
      stringBuilder.append("havingFilter").append(':').append(havingFilter).append(',');
    }
    if (sort != null) {
      stringBuilder.append("sort").append(':').append(sort).append(',');
    }
    stringBuilder.append("limit:").append(limit);
    if (!postAggregations.isEmpty()) {
      stringBuilder.append(",postAggregations:");
      int count = 0;
      for (String func : postAggregations) {
        if (count == postAggregations.size() - 1) {
          stringBuilder.append(func);
        } else {
          stringBuilder.append(func).append(", ");
        }
        count++;
      }
    }
    String brokerReduceNode = stringBuilder.append(')').toString();
    Object[] brokerReduceRow = new Object[3];
    brokerReduceRow[0] = brokerReduceNode;
    brokerReduceRow[1] = globalId[0];
    // broker reduce is root of the tree under query rewrite
    brokerReduceRow[2] = -1;

    globalId[0] = globalId[0] + 1;
    resultRows.add(brokerReduceRow);
  }
}
