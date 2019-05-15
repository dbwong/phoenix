/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.CastParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.OffsetNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.parse.TraverseNoParseNodeVisitor;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowValueConstructorOffsetNotCoercibleException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.IndexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public class OffsetCompiler {

    private final static Logger LOGGER = LoggerFactory.getLogger(OffsetCompiler.class);

    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private static final PDatum OFFSET_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PInteger.INSTANCE;
        }

        @Override
        public Integer getMaxLength() {
            return null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }
    };

    private OffsetCompiler() {}

    // eager initialization
    final private static OffsetCompiler OFFSET_COMPILER = getInstance();

    private static OffsetCompiler getInstance() {
        return new OffsetCompiler();
    }

    public static OffsetCompiler getOffsetCompiler() {
        return OFFSET_COMPILER;
    }

    public CompiledOffset compile(StatementContext context, FilterableStatement statement) throws SQLException {
        OffsetNode offsetNode = statement.getOffset();
        if (offsetNode == null) { return new CompiledOffset(Optional.absent(), Optional.absent()); }
        if (offsetNode.isIntegerOffset()) {
            OffsetParseNodeVisitor visitor = new OffsetParseNodeVisitor(context);
            offsetNode.getOffsetParseNode().accept(visitor);
            Integer offset = visitor.getOffset();
            return new CompiledOffset(Optional.fromNullable(offset), Optional.absent());
        } else {
            // We have a RVC offset. See PHOENIX-4845

            // This is a EqualParseNode with LHS and RHS RowValueConstructorParseNodes
            // This is enforced as part of the grammar
            EqualParseNode equalParseNode = (EqualParseNode)offsetNode.getOffsetParseNode();

            RowValueConstructorParseNode rvcColumnsParseNode = (RowValueConstructorParseNode)equalParseNode.getLHS();
            RowValueConstructorParseNode rvcConstantParseNode = (RowValueConstructorParseNode)equalParseNode.getRHS();

            // disallow use with aggregations
            if (statement.isAggregate()) { throw new SQLException("RVC Offset not allowed in Aggregates"); }

            // Get the tables primary keys
            if (context.getResolver().getTables().size() != 1) {
                throw new SQLException("RVC Offset not allowed with zero or multiple tables");
            }

            PTable pTable = context.getCurrentTable().getTable();

            List<PColumn> columns = pTable.getPKColumns();

            int numUserColumns = columns.size(); // columns specified by the user
            int userColumnIndex = 0; // index into the ordered list, columns, of where user specified start

            // if we are salted we need to take a subset of the pk
            Integer buckets = pTable.getBucketNum();
            if (buckets != null && buckets > 0) { // We are salted
                numUserColumns--;
                userColumnIndex++;
            }

            if (pTable.isMultiTenant() && pTable.getTenantId() != null) {
                // the tenantId is one of the pks and will be handled automatically
                numUserColumns--;
                userColumnIndex++;
            }

            boolean isIndex = false;
            if (PTableType.INDEX.equals(pTable.getType())) {
                isIndex = true;
                // If we are a view index we have to handle the idxId column
                // Note that viewIndexId comes before tenantId (what about salt byte?)
                if (pTable.getViewIndexId() != null) {
                    numUserColumns--;
                    userColumnIndex++;
                }
            }

            // Sanity check that they are providing all the user defined keys to this table
            if (numUserColumns != rvcConstantParseNode.getChildren().size()) {
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must exactly cover the tables PK.");
            }

            // Make sure the order is the same and all the user defined columns are mentioned in the column RVC
            if (numUserColumns != rvcColumnsParseNode.getChildren().size()) {
                throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset must specify the tables PKs.");
            }

            List<ColumnParseNode> rvcColumnParseNodeList = this.buildListOfColumnParseNodes(rvcColumnsParseNode, isIndex);

            if(rvcColumnParseNodeList.size() != numUserColumns) {
                throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset must specify the tables PKs.");
            }

            // We resolve the mini-where now so we can compare to tables pks PColumns and to produce a row offset
            // Construct a mini where clause
            ParseNode miniWhere = equalParseNode;

            Set<HintNode.Hint> originalHints = statement.getHint().getHints();
            WhereCompiler.WhereExpressionCompiler whereCompiler = new WhereCompiler.WhereExpressionCompiler(context);

            Expression whereExpression = miniWhere.accept(whereCompiler);

            Expression expression = WhereOptimizer.pushKeyExpressionsToScan(context, originalHints, whereExpression,
                    null, Optional.absent());
            if (expression == null) {
                LOGGER.error("Unexpected error while compiling RVC Offset, got null expression.");
                throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset unexpected failure.");
            }

            // Now that columns etc have been resolved lets check to make sure they match the pk order
            // Today the RowValueConstuctor Equality gets Rewritten into AND of EQ Ops.
            if (!(whereExpression instanceof AndExpression)) {
                LOGGER.warn("Unexpected error while compiling RVC Offset, expected AndExpression got " + whereExpression.getClass().getName());
                throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset must specify the tables PKs.");
            }

            List<RowKeyColumnExpression> rowKeyColumnExpressionList = buildListOfRowKeyColumnExpressions(whereExpression.getChildren(),isIndex);
            if(rowKeyColumnExpressionList.size() != numUserColumns) {
                LOGGER.warn("Unexpected error while compiling RVC Offset, expected " + numUserColumns + " found " + rowKeyColumnExpressionList.size());
                throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset must specify the table's PKs.");
            }

            for (int i = 0; i < numUserColumns; i++) {
                PColumn column = columns.get(i + userColumnIndex);
                //
                ColumnParseNode columnParseNode = rvcColumnParseNodeList.get(i);

                String columnParseNodeString = columnParseNode.getFullName();
                if (isIndex) {
                    columnParseNodeString = IndexUtil.getDataColumnName(columnParseNodeString);
                }

                RowKeyColumnExpression rowKeyColumnExpression = rowKeyColumnExpressionList.get(i);
                String expressionName = rowKeyColumnExpression.getName();

                // Not sure why it is getting quoted
                expressionName = expressionName.replace("\"", "");

                if (isIndex) {
                    expressionName = IndexUtil.getDataColumnName(expressionName);
                }

                if (!StringUtils.equals(expressionName, columnParseNodeString)) {
                    throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset must specify the tables PKs.");
                }

                //
                String columnString = column.getName().getString();
                if (isIndex) {
                    columnString = IndexUtil.getDataColumnName(columnString);
                }
                if (!StringUtils.equals(expressionName, columnString)) {
                    throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset must specify the tables PKs.");
                }
            }

            // check to see if this was a single key expression
            ScanRanges scanRanges = context.getScanRanges();

            if (!scanRanges.isPointLookup()) {
                throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset must be a point lookup.");
            }

            // Note the use of getLowerRange as this already handles the case of inclusionary offset
            CompiledOffset compiledOffset = new CompiledOffset(Optional.absent(),
                    Optional.of(scanRanges.getScanRange().getLowerRange()));

            return compiledOffset;
        }
    }

    @VisibleForTesting
    List<RowKeyColumnExpression> buildListOfRowKeyColumnExpressions(List<Expression> expressions, boolean isIndex) throws RowValueConstructorOffsetNotCoercibleException {
        List<RowKeyColumnExpression> rowKeyColumnExpressionList = new ArrayList<>();
        for (Expression child : expressions) {
            if (!(child instanceof ComparisonExpression)) {
                LOGGER.warn("Unexpected error while compiling RVC Offset");
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must specify the tables PKs.");
            }

            Expression possibleRowKeyColumnExpression = child.getChildren().get(0);

            // Note that since we store indexes in variable length form there may be casts from fixed types to
            // variable length
            if (isIndex) {
                if (possibleRowKeyColumnExpression instanceof CoerceExpression) {
                    // Cast today has 1 child
                    possibleRowKeyColumnExpression =
                            ((CoerceExpression) possibleRowKeyColumnExpression).getChild();
                }
            }

            if (!(possibleRowKeyColumnExpression instanceof RowKeyColumnExpression)) {
                LOGGER.warn("Unexpected error while compiling RVC Offset");
                throw new RowValueConstructorOffsetNotCoercibleException(
                        "RVC Offset must specify the tables PKs.");
            }
            rowKeyColumnExpressionList.add((RowKeyColumnExpression)possibleRowKeyColumnExpression);
        }
        return rowKeyColumnExpressionList;
    }

    @VisibleForTesting
    List<ColumnParseNode> buildListOfColumnParseNodes(RowValueConstructorParseNode rvcColumnsParseNode, boolean isIndex)  throws RowValueConstructorOffsetNotCoercibleException{
        List<ColumnParseNode> nodes = new ArrayList<>();
        for (ParseNode node : rvcColumnsParseNode.getChildren()) {
            // Note that since we store indexes in variable length form there may be casts from fixed types to
            // variable length
            if (isIndex) {
                if (node instanceof CastParseNode) {
                    // Cast today has 1 child
                    node = node.getChildren().get(0);
                }
            }

            if (!(node instanceof ColumnParseNode)) {
                throw new RowValueConstructorOffsetNotCoercibleException("RVC Offset must specify the tables PKs.");
            } else {
                nodes.add((ColumnParseNode)node);
            }
        }
        return nodes;
    }


    private static class OffsetParseNodeVisitor extends TraverseNoParseNodeVisitor<Void> {
        private final StatementContext context;
        private Integer offset;

        OffsetParseNodeVisitor(StatementContext context) {
            this.context = context;
        }

        Integer getOffset() {
            return offset;
        }

        @Override
        public Void visit(LiteralParseNode node) throws SQLException {
            Object offsetValue = node.getValue();
            if (offsetValue != null) {
                Integer offset = (Integer)OFFSET_DATUM.getDataType().toObject(offsetValue, node.getType());
                if (offset >= 0) {
                    this.offset = offset;
                }
            }
            return null;
        }

        @Override
        public Void visit(BindParseNode node) throws SQLException {
            // This is for static evaluation in SubselectRewriter.
            if (context == null) return null;

            Object value = context.getBindManager().getBindValue(node);
            context.getBindManager().addParamMetaData(node, OFFSET_DATUM);
            // Resolve the bind value, create a LiteralParseNode, and call the
            // visit method for it.
            // In this way, we can deal with just having a literal on one side
            // of the expression.
            visit(NODE_FACTORY.literal(value, OFFSET_DATUM.getDataType()));
            return null;
        }

    }

}
