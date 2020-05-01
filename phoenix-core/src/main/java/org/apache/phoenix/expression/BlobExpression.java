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
package org.apache.phoenix.expression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.LobStore;
import org.apache.phoenix.schema.types.LobStoreException;
import org.apache.phoenix.schema.types.LobStoreFactory;
import org.apache.phoenix.schema.types.PBlob;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

import static org.apache.phoenix.expression.BlobExpression.BlobMetaData.serializeBlobMetaData;

/**
 * Expression accessor for BLOB values
 */
public class BlobExpression extends LiteralExpression {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlobExpression.class);
    private final LobStore lobStore;

    public BlobExpression(Object value, SortOrder sortOrder, Determinism determinism,
            String lobStoreImpl) {
        super(value, sortOrder, determinism, PBlob.INSTANCE);
        this.lobStore = LobStoreFactory.getLobStore(lobStoreImpl);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        String lobLocation;
        try {
            lobLocation = this.lobStore.putLob((InputStream)this.getValue());
        } catch (LobStoreException lobEx) {
            throw new RuntimeException("Persisting the BLOB failed", lobEx);
        }
        BlobMetaData metaData = getBlobMetaData(lobLocation);
        byte[] serializedBlobMetaData;
        try {
            serializedBlobMetaData = serializeBlobMetaData(metaData);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize BLOB metadata", e);
        }
        ptr.set(serializedBlobMetaData);
        return true;
    }

    public InputStream getInputStream(BlobMetaData blobMetaData) {
        Preconditions.checkNotNull(blobMetaData);
        InputStream res;
        try {
            res = this.lobStore.getLob(blobMetaData.getLobLocator());
        } catch (LobStoreException lobEx) {
            LOGGER.error("Getting BLOB failed", lobEx);
            return null;
        }
        return res;
    }

    @VisibleForTesting
    BlobMetaData getBlobMetaData(String lobLocation) {
        boolean onDisk = LobStoreFactory.isOnDisk(this.lobStore);
        LobStoreFactory.SUPPORTED_FORMATS format = LobStoreFactory.getFormat(this.lobStore);
        return new BlobMetaData(onDisk,
                lobLocation,
                EnvironmentEdgeManager.currentTimeMillis(),
                format,
                UUID.randomUUID(),
                3072L);
    }

    /**
     * Stores metadata about the BLOB
     */
    public static final class BlobMetaData implements Serializable {

        private boolean onDisk;
        private String lobLocator;
        private long lastUpdatedTime;
        private LobStoreFactory.SUPPORTED_FORMATS format;
        private UUID versionNo;
        private long size;

        BlobMetaData(boolean onDisk, String lobLocator, long lastUpdatedTime,
                LobStoreFactory.SUPPORTED_FORMATS format, UUID versionNo, long size) {
            this.onDisk = onDisk;
            this.lobLocator = lobLocator;
            this.lastUpdatedTime = lastUpdatedTime;
            this.format = format;
            this.versionNo = versionNo;
            this.size = size;
        }

        public static byte[] serializeBlobMetaData(BlobMetaData blobMetaData) throws IOException {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutput out = new ObjectOutputStream(bos)) {
                out.writeObject(blobMetaData);
                return bos.toByteArray();
            }
        }

        public static BlobMetaData deserializeBlobMetaData(byte[] serializedObj) throws IOException,
                ClassNotFoundException {
            Preconditions.checkNotNull(serializedObj);
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedObj);
                    ObjectInput in = new ObjectInputStream(bis)) {
                return (BlobMetaData) in.readObject();
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Boolean.valueOf(this.onDisk).hashCode();
            result = prime * result + (this.lobLocator == null ? 0: this.lobLocator.hashCode());
            result = prime * result + Long.valueOf(this.lastUpdatedTime).hashCode();
            result = prime * result + (this.format == null ? 0 : this.format.hashCode());
            result = prime * result + (this.versionNo == null ? 0 : this.versionNo.hashCode());
            result = prime * result + Long.valueOf(this.size).hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BlobMetaData other = (BlobMetaData) obj;
            if (this.onDisk != other.onDisk) {
                return false;
            }
            if (!Strings.equals(this.lobLocator, other.lobLocator)) {
                return false;
            }
            if (this.lastUpdatedTime != other.lastUpdatedTime) {
                return false;
            }
            if (this.format != other.format) {
                return false;
            }
            if (!this.versionNo.equals(other.versionNo)) {
                return false;
            }
            if (this.size != other.size) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "BlobMetaData Object:" + super.toString() +
                    "[onDisk: " + this.onDisk + ", " +
                    "lobLocator: " + this.lobLocator + ", " +
                    "lastUpdatedTime: " + this.lastUpdatedTime + ", " +
                    "format: " + this.format + ", " +
                    "versionNo: " + this.versionNo + ", " +
                    "size: " + this.size + "]";
        }

        public boolean isOnDisk() {
            return this.onDisk;
        }

        public String getLobLocator() {
            return this.lobLocator;
        }

        public long getLastUpdatedTime() {
            return this.lastUpdatedTime;
        }

        public LobStoreFactory.SUPPORTED_FORMATS getFormat() {
            return this.format;
        }

        public UUID getVersionNo() {
            return this.versionNo;
        }

        public long getSize() {
            return this.size;
        }

    }
}
