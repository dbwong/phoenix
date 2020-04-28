/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.BytesOrBlobReferenceFunction;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.schema.types.PLong;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.phoenix.query.QueryServices.THRESHOLD_CELL_SIZE;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_THRESHOLD_CELL_SIZE;

public class BytesOrBlobReferenceParseNode extends FunctionParseNode {

    BytesOrBlobReferenceParseNode(String name, List<ParseNode> children,
            FunctionParseNode.BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context)
            throws SQLException {
        if (children.size() != 1) {
            throw new IllegalArgumentException("BytesOrBlobReferenceFunction "
                    + "takes only 1 parameter");
        }
        long sizeLimit = context.getConnection().getQueryServices()
                .getProps().getLong(THRESHOLD_CELL_SIZE, DEFAULT_THRESHOLD_CELL_SIZE);
        List<Expression> expressions = new ArrayList<>(children);

        // Add an expression to indicate the size limit
        Expression sizeLimitExpression = LiteralExpression.newConstant(sizeLimit, PLong.INSTANCE);
        expressions.add(sizeLimitExpression);
        return new BytesOrBlobReferenceFunction(expressions);
    }

}
