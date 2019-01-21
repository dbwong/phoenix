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
package org.apache.phoenix.schema.stats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixResultSet;


import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;

public class StatsPlanningIT extends BaseUniqueNamesOwnClusterIT {
    private String tableName;
    private String schemaName;
    private String fullTableName;
    private static final int DEFAULT_GUIDE_POST_WIDTH = 20;

    @BeforeClass
    public static void doSetup() throws Exception {
        //Make more than 1 region server
        NUM_SLAVES_BASE = 5;

        // enable name space mapping at global level on both client and server side
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(7);
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(
                DEFAULT_GUIDE_POST_WIDTH));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        clientProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(
                DEFAULT_GUIDE_POST_WIDTH));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }
    
    @Before
    public void generateTableNames() throws SQLException {
        schemaName = generateUniqueName();
            try (Connection conn = getConnection()) {
                conn.createStatement().execute("CREATE SCHEMA " + schemaName);
            }
        tableName = "T_" + generateUniqueName();
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);
    }

    private Connection getConnection() throws SQLException {
        return getConnection(Integer.MAX_VALUE);
    }

    private Properties getConnectionProperties(Integer statsUpdateFreq){
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.setProperty(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.setProperty(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Integer.toString(statsUpdateFreq));
        props.setProperty(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Integer.toString(5*60*1000));
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        return props;
    }

    private Connection getConnection(Integer statsUpdateFreq) throws SQLException {
        Properties props = getConnectionProperties(statsUpdateFreq);
        return DriverManager.getConnection(getUrl(), props);
    }
    
    @Test
    public void testUpdateStats() throws SQLException{
		Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = getConnection();
        conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" ( k INTEGER NOT NULL, a_string_array VARCHAR(255) ARRAY[7], b_string_array VARCHAR(255) ARRAY[7] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k)) GUIDE_POSTS_WIDTH=20000");
        String[] s;
        Array array;
        conn = upsertValues(props, fullTableName);
        // CAll the update statistics query here. If already major compaction has run this will not get executed.
        stmt = conn.prepareStatement("UPDATE STATISTICS " + fullTableName);
        stmt.execute();

        //as the explain below doesn't hit the regions it doesn't realize right away that the table
        //has been split
        rs = conn.createStatement().executeQuery(" SELECT k FROM " + fullTableName + " LIMIT 1");
        while(rs.next()){
            System.out.println(rs.getInt(1));
        }

        rs = conn.createStatement().executeQuery(" EXPLAIN SELECT k FROM " + fullTableName);

        while(rs.next()){
            System.out.println(rs.getString(1));
        }

        conn.close();
    }

    private Connection upsertValues(Properties props, String tableName) throws SQLException {
        Connection conn;
        PreparedStatement stmt;
        conn = getConnection();


        //Insert 1 Row



        stmt = upsertStmt(conn, tableName);
        stmt.setInt(1, 1);
        String[] s = new String[] { "abc   ", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc    ", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();

        int numRows = 1;

        //Double the Rows

        for(int i = 0 ; i < 17; i++) {
            String
                    upsertSelect =
                    "UPSERT INTO " + tableName + " SELECT k + " + Integer.toString(numRows)
                            + ", a_string_array, b_string_array FROM " + tableName ;

            conn.createStatement().execute(upsertSelect);
            conn.commit();

            numRows = numRows * 2;
        }


        List<byte[]> splitPoints = new ArrayList<>();

        //make some region boundaries
        List<Integer> splitPointKeys = Lists.newArrayList(0, numRows/2);


        for(Integer splitPointKey : splitPointKeys) {
            String selectStr = "EXPLAIN SELECT * FROM " + tableName + " WHERE k=" + Integer.toString(splitPointKey);

            ResultSet rs = conn.createStatement().executeQuery(selectStr);
            byte[] startRow = ((PhoenixResultSet) rs).getContext().getScan().getStartRow();

            splitPoints.add(startRow);
        }

        //

        try {
            splitTable(TableName.valueOf(schemaName,this.tableName), splitPoints);
        } catch (Exception e){
            throw new RuntimeException(e);
        }

        try {

            Admin admin = driver.getConnectionQueryServices(getUrl(), getConnectionProperties(
                    Integer.MAX_VALUE)).getAdmin();
            List<HRegionInfo>
                    tableRegions =
                    admin.getTableRegions(TableName.valueOf(schemaName, this.tableName));

            for(HRegionInfo info : tableRegions){
                System.out.println(info.toString());
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }



        return conn;
    }

    private PreparedStatement upsertStmt(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt;
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        return stmt;
    }
}
