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
package org.apache.phoenix.schema.types;

import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.expression.BlobExpression;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Types;

/**
 * Phoenix representation of the BLOB datatype
 */
public class PBlob extends PVarbinary {

    public static final PBlob INSTANCE = new PBlob();

     private PBlob() {
        super("BLOB", Types.BLOB, InputStream.class, null, 48);
    }

    @Override
    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
            SortOrder sortOrder, Integer maxLength, Integer scale) {
        Object b = super.toObject(bytes, offset, length, actualType, sortOrder, maxLength, scale);
        if (!(b instanceof byte[])) {
            throw new RuntimeException("Corrupted BLOB metadata found");
        }
        byte[] bytesCopy = (byte[])b;
        BlobExpression.BlobMetaData blobMetaData;
        try {
            blobMetaData = BlobExpression.BlobMetaData.deserializeBlobMetaData(bytesCopy);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Could not deserialize BLOB metadata", e);
        }
        if (blobMetaData != null) {
            BlobExpression dummyBlobExpr = new BlobExpression(null, null, null,
                    blobMetaData.getFormat().name());
            InputStream inputStream = dummyBlobExpr.getInputStream(blobMetaData);
            return new PhoenixBlob(blobMetaData, inputStream);
        }
        return null;
    }

}
