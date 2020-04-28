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
package org.apache.phoenix.end2end;

import org.apache.commons.io.IOUtils;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.phoenix.query.QueryServices.THRESHOLD_CELL_SIZE;
import static org.junit.Assert.assertTrue;

public class BlobTypeIT extends BaseUniqueNamesOwnClusterIT {

    private static final long configuredThreshold = 20L;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(THRESHOLD_CELL_SIZE, Long.toString(configuredThreshold));
        setUpTestDriver(new ReadOnlyProps(props));
    }

    @Test
    public void testBlobUpsertFromImageFile() throws Exception {
        String fName = "/images/dali.jpeg";
        InputStream input = BlobTypeIT.class.getResourceAsStream(fName);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + tableName +
                        " (id VARCHAR(255) not null primary key, image BLOB)");
            }
            String upsertQuery = "UPSERT INTO " + tableName + " VALUES(?, ?)";
            try (PreparedStatement prepStmt = conn.prepareStatement(upsertQuery)) {
                prepStmt.setString(1, "ID1");
                prepStmt.setBlob(2, input);
                prepStmt.execute();
            }
            conn.commit();
        }
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            TestUtil.printResultSet(stmt.executeQuery("SELECT image FROM " + tableName ));
            ResultSet rs = stmt.executeQuery("SELECT image FROM " + tableName + " WHERE id='ID1'");
            assertTrue(rs.next());
            InputStream inputStr = rs.getBinaryStream(1);
            Blob blob = rs.getBlob(1);
            assertTrue(IOUtils.contentEquals(BlobTypeIT.class.getResourceAsStream(fName), inputStr));
            assertTrue(IOUtils.contentEquals(BlobTypeIT.class.getResourceAsStream(fName), blob.getBinaryStream()));
        }
    }

    @Test
    public void testImplicitlyHandleLargeData() throws SQLException {
        String tableName = generateUniqueName();
        Random random = new Random();
        byte[] randBytes = new byte[(int) (configuredThreshold + 1)];
        random.nextBytes(randBytes);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + tableName +
                        " (id VARCHAR(255) not null primary key, image varbinary)");
            }
            String upsertQuery = "UPSERT INTO " + tableName + " VALUES(?, BYTES_OR_BLOB_REF(?))";
            try (PreparedStatement prepStmt = conn.prepareStatement(upsertQuery)) {
                prepStmt.setString(1, "ID1");
                prepStmt.setBytes(2, randBytes);
                prepStmt.execute();
            }
            conn.commit();
        }
        // Assert values upserted above
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT image FROM " + tableName +
                    " WHERE id='ID1'");
            assertTrue(rs.next());
            // TODO: Handle the implicit BLOB conversion in rs
            rs.getBytes(1);
            // TODO: Add actual test
        }
    }

}
