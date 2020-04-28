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
package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.BytesOrBlobReferenceParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;

import java.util.List;

@BuiltInFunction(name= BytesOrBlobReferenceFunction.NAME,
        nodeClass= BytesOrBlobReferenceParseNode.class,
        args={@Argument(allowedTypes={PVarbinary.class})})
public class BytesOrBlobReferenceFunction extends ScalarFunction {

    public static final String NAME = "BYTES_OR_BLOB_REF";

    public BytesOrBlobReferenceFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        List<Expression> children = getChildren();
        Expression exp = children.get(0);
        if (!exp.evaluate(tuple, ptr)) {
            return false;
        }
        byte[] data = ptr.get();
        if (data == null) {
            throw new RuntimeException("Got null bytes");
        }
        Expression sizeLimitExp = children.get(1);
        if (!sizeLimitExp.evaluate(tuple, ptr)) {
            throw new RuntimeException("No size limit for handling data provided");
        }
        long sizeLimit = sizeLimitExp.getDataType().getCodec()
                .decodeLong(ptr, sizeLimitExp.getSortOrder());
        if (sizeLimit > 0 && data.length > sizeLimit) {
            // handle as BLOB
            handAsBlob(data);
        } else {
            // do nothing and continue storing as is
            ptr.set(data);
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    private void handAsBlob(byte[] data) {
        // TODO: Create a BlobExpression and embed the actual size of the byte[] and store in metadata?
    }
}
