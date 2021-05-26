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

package org.apache.pinot.queries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.explain.ExplainPlanTreeNode;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TestExplainPlanQueries {

  IndexingConfig indexingConfig = new IndexingConfig();
  List<FieldConfig> fieldConfigList = new ArrayList<>();
  TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  @Test
  public void testExplainPlanQueries() {
    String explainPlanQuery;
    QueryContext queryContext;
    // cols using inverted index - inverted_a, inverted_b
    List<String> invertedIndexCols = new ArrayList<>();
    invertedIndexCols.add("inverted_a");
    invertedIndexCols.add("inverted_b");
    // col using sorted index - sorted_col
    List<String> sortedIndexCols = new ArrayList<>();
    sortedIndexCols.add("sorted_col");
    // cols using range index - range_a, range_b
    List<String> rangeIndexCols = new ArrayList<>();
    rangeIndexCols.add("range_a");
    rangeIndexCols.add("range_b");
    // cols using json index - json_a, json_b
    List<String> jsonIndexCols = new ArrayList<>();
    jsonIndexCols.add("json_a");
    jsonIndexCols.add("json_b");
    indexingConfig.setInvertedIndexColumns(invertedIndexCols);
    indexingConfig.setSortedColumn(sortedIndexCols);
    indexingConfig.setRangeIndexColumns(rangeIndexCols);
    indexingConfig.setJsonIndexColumns(jsonIndexCols);
    tableConfig.setIndexingConfig(indexingConfig);
    // cols using text index - foobar
    FieldConfig fieldConfig =
        new FieldConfig("foobar", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null, null);
    fieldConfigList.add(fieldConfig);
    tableConfig.setFieldConfigList(fieldConfigList);

    // set up schema for expected result table
    List<String> columnNames = new ArrayList<>();
    List<DataSchema.ColumnDataType> columnTypes = new ArrayList<>();
    columnNames.add("Operator");
    columnNames.add("Operator_Id");
    columnNames.add("Parent_Id");
    columnTypes.add(DataSchema.ColumnDataType.STRING);
    columnTypes.add(DataSchema.ColumnDataType.INT);
    columnTypes.add(DataSchema.ColumnDataType.INT);
    DataSchema dataSchema =
        new DataSchema(columnNames.toArray(new String[0]), columnTypes.toArray(new DataSchema.ColumnDataType[0]));
    List<Object[]> resultRows;
    ResultTable expectedResultTable;
    BrokerResponseNative brokerResponse;
    ResultTable actualResultTable;
    BrokerResponseNative nonTabularBrokerResponse;

    // set up broker reduce service to handle request
    Map<String, Object> properties = new HashMap<>();
    BrokerReduceService brokerReduceService = new BrokerReduceService(new PinotConfiguration(properties));

    // Test 1: select *
    explainPlanQuery = "EXPLAIN PLAN FOR SELECT * FROM testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:ALL)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "PROJECT(ALL)", 3, 2);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 4, 3);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());
//    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
//    assertTrue(nonTabularBrokerResponse.getResultTable().getRows().size() == 1);
//    String plan = (String) nonTabularBrokerResponse.getResultTable().getRows().get(0)[0];
//    String[] actualStringArray = plan.split("\n");
//    for (String s : actualStringArray) {
//      System.out.println(s.replaceAll("\\s+",""));
//    }


    // Test 2: select a subset of columns
    explainPlanQuery = "EXPLAIN PLAN FOR SELECT inverted_a, b FROM testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:inverted_a, b)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "PROJECT(inverted_a, b)", 3, 2);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 4, 3);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 3: select * query with simple filter
    explainPlanQuery = "EXPLAIN PLAN FOR SELECT * FROM testTable WHERE inverted_a = 1.5 AND sorted_col = 1";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:ALL)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "PROJECT(ALL)", 3, 2);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_a = 1.5)", 5, 4);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_a)", 6, 5);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:sorted_col = 1)", 7, 4);
    addRowToResultTable(resultRows, "SORTED_INDEX_SCAN(table:testTable_OFFLINE,column:sorted_col)", 8, 7);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 4: select * query with deeper filter (index triggered: INVERTED, SORTED, RANGE)
    explainPlanQuery = "EXPLAIN PLAN FOR SELECT * FROM testTable "
        + "WHERE (inverted_a = 1.5 AND sorted_col = 1) OR "
        + "(inverted_a BETWEEN 100 and 200 AND range_a > 20 AND sorted_col < 100)";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:ALL)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "PROJECT(ALL)", 3, 2);
    addRowToResultTable(resultRows, "FILTER(operator:OR)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_a = 1.5)", 6, 5);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_a)", 7, 6);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:sorted_col = 1)", 8, 5);
    addRowToResultTable(resultRows, "SORTED_INDEX_SCAN(table:testTable_OFFLINE,column:sorted_col)", 9, 8);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 10, 4);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:inverted_a BETWEEN 100 AND 200)", 11, 10);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:inverted_a)", 12, 11);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:range_a > 20)", 13, 10);
    addRowToResultTable(resultRows, "RANGE_INDEX_SCAN(table:testTable_OFFLINE,column:range_a)", 14, 13);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:sorted_col < 100)", 15, 10);
    addRowToResultTable(resultRows, "SORTED_INDEX_SCAN(table:testTable_OFFLINE,column:sorted_col)", 16, 15);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 5: select * query with deeper filter
    // index triggered: INVERTED, SORTED, JSON, TEXT, operators: IN, NOT IN, !=, JSON_MATCH, TEXT_MATCH
    explainPlanQuery = "EXPLAIN PLAN FOR SELECT * FROM testTable "
        + "WHERE (inverted_a IN (10, 20, 30) AND sorted_col != 100) OR "
        + "(full_a NOT IN (10, 20, 30) AND range_a != 20 AND JSON_MATCH(json_a, 'key=1') AND TEXT_MATCH(foobar, 'foo'))";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:ALL)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "PROJECT(ALL)", 3, 2);
    addRowToResultTable(resultRows, "FILTER(operator:OR)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:IN,predicate:inverted_a IN (10,20,30))", 6, 5);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_a)", 7, 6);
    addRowToResultTable(resultRows, "FILTER(operator:NOT_EQ,predicate:sorted_col != 100)", 8, 5);
    addRowToResultTable(resultRows, "SORTED_INDEX_SCAN(table:testTable_OFFLINE,column:sorted_col)", 9, 8);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 10, 4);
    addRowToResultTable(resultRows, "FILTER(operator:NOT_IN,predicate:full_a NOT IN (10,20,30))", 11, 10);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:full_a)", 12, 11);
    addRowToResultTable(resultRows, "FILTER(operator:NOT_EQ,predicate:range_a != 20)", 13, 10);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:range_a)", 14, 13);
    addRowToResultTable(resultRows, "FILTER(operator:JSON_MATCH,predicate:json_match(json_a,'key=1'))", 15, 10);
    addRowToResultTable(resultRows, "JSON_INDEX_SCAN(table:testTable_OFFLINE,column:json_a)", 16, 15);
    addRowToResultTable(resultRows, "FILTER(operator:TEXT_MATCH,predicate:text_match(foobar,'foo'))", 17, 10);
    addRowToResultTable(resultRows, "TEXT_INDEX_SCAN(table:testTable_OFFLINE,column:foobar)", 18, 17);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 6: select * query with deeper filter - full scan will be used for cols with no special indices
    explainPlanQuery = "EXPLAIN PLAN FOR SELECT * FROM testTable "
        + "WHERE (full1 IN (10, 20, 30) AND full2 != 100) OR "
        + "(full3 NOT IN (10, 20, 30) AND full4 != 20 AND JSON_MATCH(full5, 'key=1') AND TEXT_MATCH(full6, 'foo'))";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:ALL)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "PROJECT(ALL)", 3, 2);
    addRowToResultTable(resultRows, "FILTER(operator:OR)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:IN,predicate:full1 IN (10,20,30))", 6, 5);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:full1)", 7, 6);
    addRowToResultTable(resultRows, "FILTER(operator:NOT_EQ,predicate:full2 != 100)", 8, 5);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:full2)", 9, 8);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 10, 4);
    addRowToResultTable(resultRows, "FILTER(operator:NOT_IN,predicate:full3 NOT IN (10,20,30))", 11, 10);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:full3)", 12, 11);
    addRowToResultTable(resultRows, "FILTER(operator:NOT_EQ,predicate:full4 != 20)", 13, 10);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:full4)", 14, 13);
    addRowToResultTable(resultRows, "FILTER(operator:JSON_MATCH,predicate:json_match(full5,'key=1'))", 15, 10);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:full5)", 16, 15);
    addRowToResultTable(resultRows, "FILTER(operator:TEXT_MATCH,predicate:text_match(full6,'foo'))", 17, 10);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:full6)", 18, 17);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 7: simple select of subset of columns with filter, ORDER BY and LIMIT
    explainPlanQuery =
        "EXPLAIN PLAN FOR SELECT col1, col2 FROM testTable WHERE inverted_a = 1.5 AND sorted_col > 1 ORDER BY col1 DESC LIMIT 200";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:col1, col2)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(sort:[col1 DESC],limit:200)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "PROJECT(col2, col1)", 3, 2);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_a = 1.5)", 5, 4);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_a)", 6, 5);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:sorted_col > 1)", 7, 4);
    addRowToResultTable(resultRows, "SORTED_INDEX_SCAN(table:testTable_OFFLINE,column:sorted_col)", 8, 7);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 8: simple select of subset of columns with transforms, no transforms, filter, ORDER BY and LIMIT
    explainPlanQuery = "EXPLAIN PLAN FOR SELECT DATETIMECONVERT(col1), col2 "
        + "FROM testTable WHERE inverted_a = 1.5 AND sorted_col <= 1.6 ORDER BY col1 DESC LIMIT 200";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:datetimeconvert(col1), col2)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(sort:[col1 DESC],limit:200)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:datetimeconvert(col1))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(col2, col1)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_a = 1.5)", 6, 5);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_a)", 7, 6);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:sorted_col <= 1.6)", 8, 5);
    addRowToResultTable(resultRows, "SORTED_INDEX_SCAN(table:testTable_OFFLINE,column:sorted_col)", 9, 8);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 9: select * with transforms in the filter
    explainPlanQuery = "EXPLAIN PLAN FOR SELECT * FROM testTable "
        + "WHERE DIV(bar, foo) BETWEEN 10 AND 20 "
        + "AND arrayLength(col) > 2 "
        + "AND inverted_a * 5 < 1000";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:ALL)", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "PROJECT(ALL)", 3, 2);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:div(bar,foo) BETWEEN 10 AND 20)", 5, 4);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:div(bar,foo))", 6, 5);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:bar)", 7, 6);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:foo)", 8, 6);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:arraylength(col) > 2)", 9, 4);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:arraylength(col))", 10, 9);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:col)", 11, 10);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:times(inverted_a,5) < 1000)", 12, 4);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:times(inverted_a,5))", 13, 12);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:inverted_a)", 14, 13);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 10: simple aggregation without filter
    explainPlanQuery = "SELECT count(*) FROM testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:count(*))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE(aggregations:count(*))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(ALL)", 4, 3);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 5, 4);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 11: multiple aggregations without filter
    explainPlanQuery = "SELECT count(*), max(col1), sum(col2), avg(col3) FROM testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:count(*), max(col1), sum(col2), avg(col3))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE(aggregations:count(*), max(col1), sum(col2), avg(col3))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(col1, col2, col3)", 4, 3);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 5, 4);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 12: aggregations with filter
    explainPlanQuery = "SELECT count(*), max(col1), sum(col2), avg(col3) FROM testTable WHERE inverted_a = 1";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:count(*), max(col1), sum(col2), avg(col3))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE(aggregations:count(*), max(col1), sum(col2), avg(col3))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(col1, col2, col3)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_a = 1)", 5, 4);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_a)", 6, 5);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 13: transforms within aggregations without filter
    explainPlanQuery = "SELECT sum(add(col1, col2)), MIN(ADD(DIV(col1,col2),DIV(col3,col4))) FROM testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:sum(add(col1,col2)), min(add(div(col1,col2),div(col3,col4))))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE(aggregations:sum(add(col1,col2)), min(add(div(col1,col2),div(col3,col4))))", 3, 2);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:add(col1,col2), add(div(col1,col2),div(col3,col4)))", 4, 3);
    addRowToResultTable(resultRows, "PROJECT(col1, col2, col3, col4)", 5, 4);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 6, 5);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 14: transform within aggregations with filter containing transforms
    explainPlanQuery = "SELECT sum(add(col1, col2)), MIN(ADD(DIV(col1,col2),DIV(col3,col4))) FROM testTable "
        + "WHERE arrayMax(col) > 2";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:sum(add(col1,col2)), min(add(div(col1,col2),div(col3,col4))))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE(aggregations:sum(add(col1,col2)), min(add(div(col1,col2),div(col3,col4))))", 3, 2);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:add(col1,col2), add(div(col1,col2),div(col3,col4)))", 4, 3);
    addRowToResultTable(resultRows, "PROJECT(col1, col2, col3, col4)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:arraymax(col) > 2)", 6, 5);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:arraymax(col))", 7, 6);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:col)", 8, 7);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 15: aggregation group-by on single column
    explainPlanQuery = "SELECT col2, max(col1), min(col4) FROM testTable WHERE inverted_b = 10 GROUP BY col2";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:col2, max(col1), min(col4))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:col2,aggregations:min(col4), max(col1))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(col4, col2, col1)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_b = 10)", 5, 4);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_b)", 6, 5);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 16: aggregation group-by on multiple columns
    explainPlanQuery = "SELECT col1, col2, max(col3), min(col4) FROM testTable "
        + "WHERE TEXT_MATCH(foobar, 'foo') GROUP BY col1, col2";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:col1, col2, max(col3), min(col4))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:col1, col2,aggregations:max(col3), min(col4))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(col4, col2, col1, col3)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:TEXT_MATCH,predicate:text_match(foobar,'foo'))", 5, 4);
    addRowToResultTable(resultRows, "TEXT_INDEX_SCAN(table:testTable_OFFLINE,column:foobar)", 6, 5);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 17: aggregation group-by on multiple columns, order by column and aggregations, limit
    explainPlanQuery = "SELECT col1, col2, max(col3), min(col4) FROM testTable "
        + "WHERE TEXT_MATCH(foobar, 'foo') GROUP BY col1, col2 ORDER BY col1, max(col3) desc limit 200";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:col1, col2, max(col3), min(col4))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(sort:[col1 ASC, max(col3) DESC],limit:200)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:col1, col2,aggregations:max(col3), min(col4))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(col4, col2, col1, col3)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:TEXT_MATCH,predicate:text_match(foobar,'foo'))", 5, 4);
    addRowToResultTable(resultRows, "TEXT_INDEX_SCAN(table:testTable_OFFLINE,column:foobar)", 6, 5);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 18: transform in group-by, aggregation, and filter
    explainPlanQuery = "SELECT DATETIMECONVERT(col2), sum(add(col1, col2)), min(col4) FROM testTable "
        + "WHERE arrayMin(col) < 100 GROUP BY DATETIMECONVERT(col2)";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:datetimeconvert(col2), sum(add(col1,col2)), min(col4))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:datetimeconvert(col2),aggregations:sum(add(col1,col2)), min(col4))", 3, 2);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:datetimeconvert(col2), add(col1,col2))", 4, 3);
    addRowToResultTable(resultRows, "PROJECT(col4, col2, col1)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:arraymin(col) < 100)", 6, 5);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:arraymin(col))", 7, 6);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:col)", 8, 7);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 19: aggregation, transform in group-by, order-by
    explainPlanQuery = "SELECT max(col1), min(col4), DATETIMECONVERT(col2) "
        + "FROM testTable "
        + "WHERE inverted_b = 10 "
        + "GROUP BY DATETIMECONVERT(col2) "
        + "ORDER BY max(col1) DESC";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:max(col1), min(col4), datetimeconvert(col2))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(sort:[max(col1) DESC],limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:datetimeconvert(col2),aggregations:min(col4), max(col1))", 3, 2);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:datetimeconvert(col2))", 4, 3);
    addRowToResultTable(resultRows, "PROJECT(col4, col1, col2)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_b = 10)", 6, 5);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_b)", 7, 6);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 20: aggregation, transform in group-by, order-by, having
    explainPlanQuery = "SELECT max(col1), min(col4), DATETIMECONVERT(col2) "
        + "FROM testTable "
        + "WHERE inverted_b = 10 "
        + "GROUP BY DATETIMECONVERT(col2) "
        + "HAVING max(col1) > 20"
        + "ORDER BY max(col1) DESC";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:max(col1), min(col4), datetimeconvert(col2))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(havingFilter:max(col1) > 20,sort:[max(col1) DESC],limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:datetimeconvert(col2),aggregations:max(col1), min(col4))", 3, 2);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:datetimeconvert(col2))", 4, 3);
    addRowToResultTable(resultRows, "PROJECT(col4, col2, col1)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_b = 10)", 6, 5);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_b)", 7, 6);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 21: distinct
    explainPlanQuery = "SELECT DISTINCT col1, col2 FROM testTable WHERE inverted_b = 10";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:distinct(col1,col2))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "DISTINCT(keyColumns:col1, col2)", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(col2, col1)", 4, 3);
    addRowToResultTable(resultRows, "FILTER(operator:EQ,predicate:inverted_b = 10)", 5, 4);
    addRowToResultTable(resultRows, "INVERTED_INDEX_SCAN(table:testTable_OFFLINE,column:inverted_b)", 6, 5);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 22: Alias Agg Group-by Order-by Having Transform
    explainPlanQuery = "SELECT col1, DATETIMECONVERT(col2), sum(col3) as col3_sum, count(*), distinctCount(col4) "
        + "FROM testTable "
        + "WHERE col5 in ('a', 'b', 'c') AND col6 not in (1, 2, 3) "
        + "GROUP BY col1, DATETIMECONVERT(col2) "
        + "HAVING sum(col3) > 100 "
        + "ORDER BY col1 DESC LIMIT 100";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:col1, datetimeconvert(col2), col3_sum, count(*), distinctcount(col4))", 0, -1);
    addRowToResultTable(resultRows, "UPDATE_ALIAS(sum(col3)->col3_sum)", 1, 0);
    addRowToResultTable(resultRows, "BROKER_REDUCE(havingFilter:sum(col3) > 100,sort:[col1 DESC],limit:100)", 2, 1);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 3, 2);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:col1,datetimeconvert(col2),aggregations:count(*), sum(col3), distinctcount(col4))", 4, 3);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:datetimeconvert(col2))", 5, 4);
    addRowToResultTable(resultRows, "PROJECT(col4, col2, col1, col3)", 6, 5);
    addRowToResultTable(resultRows, "FILTER(operator:AND)", 7, 6);
    addRowToResultTable(resultRows, "FILTER(operator:IN,predicate:col5 IN ('a','b','c'))", 8, 7);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:col5)", 9, 8);
    addRowToResultTable(resultRows, "FILTER(operator:NOT_IN,predicate:col6 NOT IN (1,2,3))", 10, 7);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:col6)", 11, 10);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 23: Transform Order-by
    explainPlanQuery = "SELECT ADD(foo, ADD(bar, 123)), SUB('456', foobar) "
        + "FROM testTable "
        + "ORDER BY SUB(456, foobar) "
        + "LIMIT 30";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:add(foo,add(bar,123)), sub(456,foobar))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(sort:[sub(456,foobar) ASC],limit:30)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:sub(456,foobar), add(foo,add(bar,123)))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(bar, foobar, foo)", 4, 3);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 5, 4);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 24: Aggregation Group-by with Transform, Order-by
    explainPlanQuery = "SELECT SUB(foo, bar), bar, SUM(ADD(foo, bar)) "
        + "FROM testTable "
        + "GROUP BY SUB(foo, bar), bar "
        + "ORDER BY SUM(ADD(foo, bar)), SUB(foo, bar) DESC "
        + "LIMIT 20";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:sub(foo,bar), bar, sum(add(foo,bar)))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(sort:[sum(add(foo,bar)) ASC, sub(foo,bar) DESC],limit:20)", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:sub(foo,bar),bar,aggregations:sum(add(foo,bar)))", 3, 2);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:sub(foo,bar), add(foo,bar))", 4, 3);
    addRowToResultTable(resultRows, "PROJECT(foo, bar)", 5, 4);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 6, 5);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 25: case
    explainPlanQuery = "SELECT CASE WHEN inverted_a> 30 THEN 3 WHEN inverted_a> 20 THEN 2 WHEN inverted_a> 10 THEN 1 ELSE 0 END "
        + "AS a_category FROM testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:a_category)", 0, -1);
    addRowToResultTable(resultRows, "UPDATE_ALIAS(case(greater_than(inverted_a,30),greater_than(inverted_a,20),greater_than(inverted_a,10),3,2,1,0)->a_category)", 1, 0);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 2, 1);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 3, 2);
    // special case in transform that can mess up the nondeterministic function
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:case(greater_than(inverted_a,30),greater_than(inverted_a,20),greater_than(inverted_a,10),3,2,1,0))", 4, 3);
    addRowToResultTable(resultRows, "PROJECT(inverted_a)", 5, 4);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 6, 5);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 26: invoke compile time functions
    explainPlanQuery = "SELECT col1, 5+5 as currentTs from testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "QUERY_REWRITE", 0, -1);
    addRowToResultTable(resultRows, "INVOKE_COMPILATION_TIME_FUNCTIONS([PLUS(5.0, 5.0)->10.0])", 1, 0);
    addRowToResultTable(resultRows, "SELECT(selectList:col1, currentTs)", 2, -1);
    addRowToResultTable(resultRows, "UPDATE_ALIAS(10.0->currentTs)",3, 2);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 4, 3);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 5, 4);
    addRowToResultTable(resultRows, "PROJECT(col1)", 6, 5);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 7, 6);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 27: rewrite selection function
    explainPlanQuery = "SELECT sum(array_sum(a)) as newCol1 FROM testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "QUERY_REWRITE", 0, -1);
    addRowToResultTable(resultRows, "REWRITE_SELECTIONS([sum(array_sum(a))->summv(a)])", 1, 0);
    addRowToResultTable(resultRows, "SELECT(selectList:newCol1)", 2, -1);
    addRowToResultTable(resultRows, "UPDATE_ALIAS(summv(a)->newCol1)",3, 2);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 4, 3);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 5, 4);
    addRowToResultTable(resultRows, "AGGREGATE(aggregations:summv(a))", 6, 5);
    addRowToResultTable(resultRows, "PROJECT(a)", 7, 6);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 8, 7);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());


    // Test 28:
    // 1. Compile time invoked
    // 2. Rewrite selection: rewrite array functions to MV functions
    // 3. Update Predicate Comparison (RHS must be literal for having and where)
    // 4. Update Ordinals (for group by and order by)
    // 5. Update alias => swtich to funcs & cols from the original table
    explainPlanQuery = "SELECT col1, 6+8 as col2, sum(array_sum(a)) as col3, min(array_min(b)), max(array_max(c)) "
        + "FROM testTable "
        + "WHERE a > b + 5 GROUP BY 2, 1 ORDER BY col3";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "QUERY_REWRITE", 0, -1);
    addRowToResultTable(resultRows, "INVOKE_COMPILATION_TIME_FUNCTIONS([PLUS(6.0, 8.0)->14.0])", 1, 0);
    addRowToResultTable(resultRows, "REWRITE_SELECTIONS([sum(array_sum(a))->summv(a), min(array_min(b))->minmv(b), max(array_max(c))->maxmv(c)])", 2, 0);
    addRowToResultTable(resultRows, "UPDATE_COMPARISON_PREDICATES([greater_than(a,plus(b,5))->greater_than(minus(a,plus(b,5)),0)])",3, 0);
    addRowToResultTable(resultRows, "APPLY_ORDINALS([2->14.0, 1->col1])", 4, 0);
    addRowToResultTable(resultRows, "APPLY_ALIASES([col3->summv(a)])", 5, 0);
    addRowToResultTable(resultRows, "SELECT(selectList:col1, col2, col3, minmv(b), maxmv(c))", 6, -1);
    addRowToResultTable(resultRows, "UPDATE_ALIAS(14.0->col2, summv(a)->col3)", 7, 6);
    addRowToResultTable(resultRows, "BROKER_REDUCE(sort:[summv(a) ASC],limit:10)", 8, 7);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 9, 8);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:col1,aggregations:summv(a), minmv(b), maxmv(c))", 10, 9);
    addRowToResultTable(resultRows, "PROJECT(a, b, c, col1)", 11, 10);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:minus(a,plus(b,5)) > 0)", 12, 11);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:minus(a,plus(b,5)))", 13, 12);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:a)", 14, 13);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:b)", 15, 13);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 29: Non-aggregation Group-by
    explainPlanQuery = "SELECT col1 FROM testTable GROUP BY col1";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "QUERY_REWRITE", 0, -1);
    addRowToResultTable(resultRows, "REWRITE_NON_AGGREGATION_GROUPBY_TO_DISTINCT", 1, 0);
    addRowToResultTable(resultRows, "SELECT(selectList:distinct(col1))", 2, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10)", 3, 2);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 4, 3);
    addRowToResultTable(resultRows, "DISTINCT(keyColumns:col1)", 5, 4);
    addRowToResultTable(resultRows, "PROJECT(col1)", 6, 5);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 7, 6);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);

    // Test 30: post aggregation
    explainPlanQuery = "SELECT SUM(col1) * SUM(col2), 5 * MAX(col2), SUM(col1) + SUM(col2) FROM testTable";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:times(sum(col1),sum(col2)), times(5,max(col2)), plus(sum(col1),sum(col2)))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(limit:10,postAggregations:times(sum(col1),sum(col2)), plus(sum(col1),sum(col2)), times(5,max(col2)))", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE(aggregations:sum(col1), sum(col2), max(col2))", 3, 2);
    addRowToResultTable(resultRows, "PROJECT(col2, col1)", 4, 3);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE)", 5, 4);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());

    // Test 31: post aggregations with regular transformation functions in select, group-by, having
    explainPlanQuery = "SELECT DATETIMECONVERT(col2), sum(add(col1, col2)), min(col4) * 5 FROM testTable "
        + "WHERE arrayMin(col) < 100 GROUP BY DATETIMECONVERT(col2) "
        + "HAVING min(col4) > 200 AND ADD(DIV(sum(add(col1, col2)), 100), 10) < 300 ";
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(explainPlanQuery);
    resultRows = new ArrayList<>();
    addRowToResultTable(resultRows, "SELECT(selectList:datetimeconvert(col2), sum(add(col1,col2)), times(min(col4),5))", 0, -1);
    addRowToResultTable(resultRows, "BROKER_REDUCE(havingFilter:(min(col4) > 200 AND add(div(sum(add(col1,col2)),100),10) < 300),limit:10,postAggregations:times(min(col4),5), add(div(sum(add(col1,col2)),100),10))", 1, 0);
    addRowToResultTable(resultRows, "SERVER_COMBINE", 2, 1);
    addRowToResultTable(resultRows, "AGGREGATE_GROUPBY(groupKeys:datetimeconvert(col2),aggregations:sum(add(col1,col2)), min(col4))", 3, 2);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:add(col1,col2), datetimeconvert(col2))", 4, 3);
    addRowToResultTable(resultRows, "PROJECT(col4, col2, col1)", 5, 4);
    addRowToResultTable(resultRows, "FILTER(operator:RANGE,predicate:arraymin(col) < 100)", 6, 5);
    addRowToResultTable(resultRows, "APPLY_TRANSFORM(transformFuncs:arraymin(col))", 7, 6);
    addRowToResultTable(resultRows, "FULL_SCAN(table:testTable_OFFLINE,column:col)", 8, 7);
    expectedResultTable = new ResultTable(dataSchema, resultRows);
    brokerResponse = brokerReduceService.reduceExplainPlanQueryOutput(queryContext, tableConfig);
    actualResultTable = brokerResponse.getResultTable();
    checkResultTable(expectedResultTable, actualResultTable);
    nonTabularBrokerResponse = brokerReduceService.reduceExplainPlanQueryOutputNontabular(queryContext, tableConfig);
    checkNonTabularResultTable(expectedResultTable, nonTabularBrokerResponse.getResultTable());
  }

  /**
   * Helper function that adds rows in expected result table
   */
  private void addRowToResultTable(List<Object[]> resultRows, String firstCol, int operatorId, int parentId) {
    Object[] resultRow = new Object[3];
    resultRow[0] = firstCol;
    resultRow[1] = operatorId;
    resultRow[2] = parentId;
    resultRows.add(resultRow);
  }

  /**
   * Helper function that checks rows in expected result table and actual result table are identical
   */
  private void checkResultTable(ResultTable expected, ResultTable actual) {
    assertEquals(expected.getDataSchema(), actual.getDataSchema());
    assertEquals(expected.getRows().size(), actual.getRows().size());
    List<Object[]> expectedRows = expected.getRows();
    List<Object[]> actualRows = actual.getRows();
    for (int i = 0; i < expectedRows.size(); i++) {
      Object[] expectedRow = expectedRows.get(i);
      Object[] actualRow = actualRows.get(i);
      Assert.assertEquals(expectedRow.length, actualRow.length);
      for (int j = 0; j < actualRow.length; j++) {
        Object actualColValue = actualRow[j];
        Object expectedColValue = expectedRow[j];
        if (j == 0) {
          String actualString = (String) actualColValue;
          String expectedString = (String) expectedColValue;
          try {
            Assert.assertEquals(expectedColValue, actualColValue);
          } catch (AssertionError e) {
            boolean bothProject = actualString.contains("PROJECT") && expectedString.contains("PROJECT");
            boolean bothAggregationGroupBy = actualString.contains("AGGREGATE_GROUPBY") && expectedString.contains("AGGREGATE_GROUPBY");
            boolean bothAggregation = actualString.contains("AGGREGATE") && expectedString.contains("AGGREGATE");
            boolean bothApplyTransform = actualString.contains("APPLY_TRANSFORM") && expectedString.contains("APPLY_TRANSFORM");
            boolean bothBrokerReduce = actualString.contains("BROKER_REDUCE") && expectedString.contains("BROKER_REDUCE");
            boolean bothQueryRewriteOperations =
                (actualString.contains(CalciteSqlParser.INVOKE_COMPILATION_TIME_FUNCTIONS) && expectedString.contains(CalciteSqlParser.INVOKE_COMPILATION_TIME_FUNCTIONS))
                    || (actualString.contains(CalciteSqlParser.REWRITE_SELECTIONS) && expectedString.contains(CalciteSqlParser.REWRITE_SELECTIONS))
                    || (actualString.contains(CalciteSqlParser.UPDATE_COMPARISON_PREDICATES) && expectedString.contains(CalciteSqlParser.UPDATE_COMPARISON_PREDICATES))
                    || (actualString.contains(CalciteSqlParser.APPLY_ORDINALS) && expectedString.contains(CalciteSqlParser.APPLY_ORDINALS))
                    || (actualString.contains(CalciteSqlParser.APPLY_ALIAS) && expectedString.contains(CalciteSqlParser.APPLY_ALIAS));
            if (bothProject) {
              compareProject(actualString, expectedString);
            } else if (bothAggregationGroupBy || bothBrokerReduce) {
              compareAggGroupByOrBrokerReduce(actualString, expectedString);
            } else if (bothApplyTransform || bothAggregation) {
              compareTransformOrAggOnly(actualString, expectedString);
            } else if (bothQueryRewriteOperations) {
              compareQueryWrite(actualString, expectedString);
            } else {
              throw e;
            }
          }
        }
      }
    }
  }

  /**
   * Helper function that checks every row in the plan of expected result table matches with
   * every row in the actual result table (nonTabularFormat)
   */
  private void checkNonTabularResultTable(ResultTable expected, ResultTable actual) {
    assertTrue(actual.getRows().size() == 1);
    assertTrue(actual.getRows().get(0)[0] instanceof String);
    String actualPlan = (String) actual.getRows().get(0)[0];
    String[] actualStringArray = actualPlan.split("\n");
    List<Object[]> expectedRows = expected.getRows();
    assertEquals(expectedRows.size(), actualStringArray.length);
    for (int i = 0; i < actualStringArray.length; i++) {
      String actualString = actualStringArray[i].trim();
      String expectedString = (String) expectedRows.get(i)[0];
      try {
        Assert.assertEquals(expectedString, actualString);
      } catch (AssertionError e) {
        boolean bothProject = actualString.contains("PROJECT") && expectedString.contains("PROJECT");
        boolean bothAggregationGroupBy = actualString.contains("AGGREGATE_GROUPBY") && expectedString.contains("AGGREGATE_GROUPBY");
        boolean bothAggregation = actualString.contains("AGGREGATE") && expectedString.contains("AGGREGATE");
        boolean bothApplyTransform = actualString.contains("APPLY_TRANSFORM") && expectedString.contains("APPLY_TRANSFORM");
        boolean bothBrokerReduce = actualString.contains("BROKER_REDUCE") && expectedString.contains("BROKER_REDUCE");
        boolean bothQueryRewriteOperations =
            (actualString.contains(CalciteSqlParser.INVOKE_COMPILATION_TIME_FUNCTIONS) && expectedString.contains(CalciteSqlParser.INVOKE_COMPILATION_TIME_FUNCTIONS))
                || (actualString.contains(CalciteSqlParser.REWRITE_SELECTIONS) && expectedString.contains(CalciteSqlParser.REWRITE_SELECTIONS))
                || (actualString.contains(CalciteSqlParser.UPDATE_COMPARISON_PREDICATES) && expectedString.contains(CalciteSqlParser.UPDATE_COMPARISON_PREDICATES))
                || (actualString.contains(CalciteSqlParser.APPLY_ORDINALS) && expectedString.contains(CalciteSqlParser.APPLY_ORDINALS))
                || (actualString.contains(CalciteSqlParser.APPLY_ALIAS) && expectedString.contains(CalciteSqlParser.APPLY_ALIAS));
        if (bothProject) {
          compareProject(actualString, expectedString);
        } else if (bothAggregationGroupBy || bothBrokerReduce) {
          compareAggGroupByOrBrokerReduce(actualString, expectedString);
        } else if (bothApplyTransform || bothAggregation) {
          compareTransformOrAggOnly(actualString, expectedString);
        } else if (bothQueryRewriteOperations) {
          compareQueryWrite(actualString, expectedString);
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Helper function that handles nondeterministic ordering of projected cols
   */
  private void compareProject(String actualString, String expectedString) {
    Set<String> actualStringSet = new HashSet<>();
    Set<String> expectedStringSet = new HashSet<>();
    String[] actualStringArray;
    String[] expectedStringArray;
    actualStringArray = actualString.split("\\(");
    expectedStringArray = expectedString.split("\\(");
    actualString = actualStringArray[1].substring(0, actualStringArray[1].length() - 1);
    expectedString = expectedStringArray[1].substring(0, expectedStringArray[1].length() - 1);
    actualStringArray = actualString.split(", ");
    expectedStringArray = expectedString.split(", ");
    assertEquals(expectedStringArray.length, actualStringArray.length);
    for (int k = 0; k < actualStringArray.length; k++) {
      actualStringSet.add(actualStringArray[k]);
      expectedStringSet.add(expectedStringArray[k]);
    }
    assertEquals(expectedStringSet, actualStringSet);
  }

  /**
   * Helper function that handles nondeterministic ordering of aggregation functions in AggregateGroupByNodes
   * or the post aggregations in BrokerReduceNodes
   */
  private void compareAggGroupByOrBrokerReduce(String actualString, String expectedString) {
    Set<String> actualStringSet = new HashSet<>();
    Set<String> expectedStringSet = new HashSet<>();
    String[] actualStringArray;
    String[] expectedStringArray;
    actualStringArray = actualString.split(":");
    expectedStringArray = expectedString.split(":");
    actualString = actualStringArray[2].substring(0, actualStringArray[2].length() - 1);
    expectedString = expectedStringArray[2].substring(0, expectedStringArray[2].length() - 1);
    actualStringArray = actualString.split(", ");
    expectedStringArray = expectedString.split(", ");
    assertEquals(expectedStringArray.length, actualStringArray.length);
    for (int k = 0; k < actualStringArray.length; k++) {
      actualStringSet.add(actualStringArray[k]);
      expectedStringSet.add(expectedStringArray[k]);
    }
  }

  /**
   * Helper function that handle nondeterministic ordering of transform functions in ApplyTransformNodes
   * or of aggregation functions in AggregateNodes
   */
  private void compareTransformOrAggOnly(String actualString, String expectedString) {
    Set<String> actualStringSet = new HashSet<>();
    Set<String> expectedStringSet = new HashSet<>();
    String[] actualStringArray;
    String[] expectedStringArray;
    actualStringArray = actualString.split(":");
    expectedStringArray = expectedString.split(":");
    actualString = actualStringArray[1].substring(0, actualStringArray[1].length() - 1);
    expectedString = expectedStringArray[1].substring(0, expectedStringArray[1].length() - 1);
    actualStringArray = actualString.split(", ");
    expectedStringArray = expectedString.split(", ");
    assertEquals(expectedStringArray.length, actualStringArray.length);
    for (int k = 0; k < actualStringArray.length; k++) {
      actualStringSet.add(actualStringArray[k]);
      expectedStringSet.add(expectedStringArray[k]);
    }
    assertEquals(expectedStringSet, actualStringSet);
  }

  /**
   * Helper function that handle nondeterministic ordering in query write operations
   */
  private void compareQueryWrite(String actualString, String expectedString) {
    Set<String> actualStringSet = new HashSet<>();
    Set<String> expectedStringSet = new HashSet<>();
    String[] actualStringArray;
    String[] expectedStringArray;
    actualStringArray = actualString.split("\\[");
    expectedStringArray = expectedString.split("\\[");
    actualString = actualStringArray[1].substring(0, actualStringArray[1].length() - 2);
    expectedString = expectedStringArray[1].substring(0, expectedStringArray[1].length() - 2);
    actualStringArray = actualString.split(", ");
    expectedStringArray = expectedString.split(", ");
    assertEquals(expectedStringArray.length, actualStringArray.length);
    for (int k = 0; k < actualStringArray.length; k++) {
      actualStringSet.add(actualStringArray[k]);
      expectedStringSet.add(expectedStringArray[k]);
    }
    assertEquals(expectedStringSet, actualStringSet);
  }
}
