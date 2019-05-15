package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// RVC Based Offset - Tests
public class RowValueConstructorOffsetIT extends ParallelStatsDisabledIT {

    static final String SIMPLE_DDL = "CREATE TABLE %s (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
            + "k2 INTEGER NOT NULL,\n" + "v1 INTEGER,\n" + "v2 VARCHAR,\n"
            + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) ";

    static final String DATA_DDL = "CREATE TABLE %s (k1 TINYINT NOT NULL,\n" + "k2 TINYINT NOT NULL,\n"
            + "k3 TINYINT NOT NULL,\n" + "v1 INTEGER,\n" + "CONSTRAINT pk PRIMARY KEY (k1, k2, k3)) ";

    String tableName = "T_" + generateUniqueName();

    String dataTableName = "T_" + generateUniqueName();

    String indexName =  "INDEX_" + tableName;

    String dataIndexName =  "INDEX_" + dataTableName;

    boolean initialized = false;
    Connection conn;

    @Before
    public void init() throws SQLException {
        if (!initialized) {
            conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

            String dataTableDDL = String.format(DATA_DDL,dataTableName);

            conn.createStatement().execute(dataTableDDL);

            conn.createStatement().execute(String.format(SIMPLE_DDL,tableName));

            conn.commit();

            String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?)", dataTableName);

            int nRows = 0;
            PreparedStatement ps = conn.prepareStatement(upsertDML);
            for (int k1 = 0; k1 < 4; k1++) {
                ps.setInt(1, k1);
                for (int k2 = 0; k2 < 4; k2++) {
                    ps.setInt(2, k2);
                    for (int k3 = 0; k3 < 4; k3++) {
                        ps.setInt(3, k3);
                        ps.setInt(4, nRows);
                        int result = ps.executeUpdate();
                        assertEquals(1, result);
                        nRows++;
                    }
                }
            }
            conn.commit();

            String createIndex = "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + " (k2 DESC,k1)";
            conn.createStatement().execute(createIndex);


            String createDataIndex = "CREATE INDEX IF NOT EXISTS " + dataIndexName + " ON " + dataTableName + " (k2 DESC,k1)";
            conn.createStatement().execute(createDataIndex);
            initialized = true;

            conn.commit();
        }
    }

    // Test RVC Offset columns must be coercible to a base table
    @Test
    public void testRVCOffsetNotCoercible() throws SQLException {
        String failureSql = String.format("SELECT t_id, k1, k2 FROM %s OFFSET (t_id, k1, k2)=('a', 'ab', 2)", tableName);
        try (Statement statement = conn.createStatement()){
            statement.execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not allow non coercible values to PK in RVC Offset");
    }

    // Test Order By Not PK Order By Exception
    @Test
    public void testRVCOffsetNotAllowNonPKOrderBy() throws SQLException {
        String failureSql = String.format("SELECT t_id, k1, k2, v1 FROM %s ORDER BY v1 OFFSET (t_id, k1, k2)=('a', 1, 2)", tableName);
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not allow no PK order by with RVC Offset");
    }

    // Test Order By Partial PK Order By Exception
    @Test
    public void testRVCOffsetNotAllowPartialPKOrderBy() throws SQLException {
        String failureSql = String.format("SELECT t_id, k1, k2 FROM %s ORDER BY k1 OFFSET (t_id, k1, k2)=('a', 1, 2)", tableName);
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            assertEquals("Do not allow non-pk ORDER BY with RVC OFFSET", e.getMessage());
            return;
        }
        fail("Should not allow partial PK order by with RVC Offset");
    }

    // Test Order By Not PK Order By Exception
    @Test
    public void testRVCOffsetNotAllowDifferentPKOrderBy() throws SQLException {
        String failureSql = String.format("SELECT t_id, k1, k2 FROM %s ORDER BY t_id DESC, k1, k2 OFFSET (k1,k2,k3)=('a', 1, 2)",
                tableName);
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not allow different PK order by with RVC Offset");
    }

    // Test Not allow joins
    @Test
    public void testRVCOffsetNotAllowedInJoins() throws SQLException {
        String tableName2 = "T_" + generateUniqueName();
        createTestTable(getUrl(), String.format(SIMPLE_DDL,tableName2));

        String failureSql = String.format("SELECT T1.k1,T2.k2 FROM %s AS T1, %s AS T2 WHERE T1.t_id=T2.t_id OFFSET (k1,k2,k3)=('a', 1, 2)",
                tableName, tableName2); // literal works
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have JOIN in RVC Offset");
    }

    // Test Not allowed in subsquery
    @Test
    public void testRVCOffsetNotAllowedInSubQuery() throws SQLException {
        String failureSql = String.format("SELECT B.k2 FROM (SELECT t_id, k2 FROM %s OFFSET (k1,k2,k3)=('a', 1, 2)) AS B", tableName);
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have subquery with RVC Offset");
    }

    // Test Not allowed on subsquery
    @Test
    public void testRVCOffsetNotAllowedOnSubQuery() throws SQLException {
        String failureSql = String.format("SELECT * FROM (SELECT t_id, k2 FROM %s) OFFSET (k1,k2,k3)=('a', 1, 2)", tableName);
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have subquery with RVC Offset");
    }

    // Test RVC Offset must be a literal, cannot have column reference
    @Test
    public void testRVCOffsetLiteral() throws SQLException {
        String failureSql = "SELECT * FROM " + tableName + "  OFFSET (k1,k2,k3)=('a', 1, k2)"; // column doesn't works
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have allowed column in RVC Offset");
    }

    // Test RVC Offset must be in non-aggregate
    @Test
    public void testRVCOffsetAggregate() throws SQLException {
        String failureSql = "SELECT count(*) FROM " + tableName + "  OFFSET (k1,k2,k3)=('a', 1, k2)";
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have allowed aggregate with RVC Offset");
    }

    // Test RVC Offset must have an entry for every one in the pk
    @Test
    public void testRVCOffsetPartialKey() throws SQLException {
        String failureSql = "SELECT * FROM " + tableName + "  OFFSET (k1,k2,k3)=('a', 1)";
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have allowed partial Key RVC Offset");
    }

    // Test RVC Offset must have an entry for every one in the pk
    @Test
    public void testRVCOffsetMoreThanKey() throws SQLException {
        String failureSql = "SELECT * FROM " + tableName + "  OFFSET (k1,k2,v1)=('a', 1, 2, 3)";
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have allowed more than pk columns in Key RVC Offset");
    }

    // Test RVC Offset simple case
    @Test
    public void testRVCOffsetLHSDoesNotMatchTable() throws SQLException {
        String failureSql = "SELECT * FROM " + dataTableName + "  LIMIT 2 OFFSET (k1,k2)=(2, 3, 1)";
        try {
            conn.createStatement().execute(failureSql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have allowed the LHS to not be the same as the pk");
    }

    // Test RVC Offset simple case
    @Test
    public void testSimpleRVCOffsetLookup() throws SQLException {
        String sql = "SELECT * FROM " + dataTableName + "  LIMIT 2 OFFSET (k1,k2,k3)=(2, 3, 2)";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(2, k3);
        }
        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(3, k3);
        }
        assertFalse(rs.next());
    }

    @Test
    public void testBindsRVCOffsetLookup() throws SQLException {
        String sql = "SELECT * FROM " + dataTableName + "  LIMIT 2 OFFSET (k1,k2,k3)=(?, ?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);

        ps.setInt(1,2);
        ps.setInt(2,3);
        ps.setInt(3,2);
        ResultSet rs = ps.executeQuery();

        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(2, k3);
        }
        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(3, k3);
        }
        assertFalse(rs.next());

        //TODO try index bind path
    }



    // Test RVC Offset where clause
    @Test
    public void testWhereClauseRVCOffsetLookup() throws SQLException {
       //Offset should not overcome the where clause
        String sql = "SELECT * FROM " + dataTableName + " WHERE (k1,k2,k3)=(3,3,3) LIMIT 2 OFFSET (k1,k2,k3)=(2, 3, 1)";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            assertEquals(3, k1);
            assertEquals(3, k2);
            assertEquals(3, k3);
        }
        assertFalse(rs.next());
    }

    @Test
    public void testSaltedTableRVCOffsetOrderBy() throws SQLException {
        //Make a salted table
        String saltedTableName = "T_" + generateUniqueName();

        //
        String saltedDDL = String.format(DATA_DDL + "SALT_BUCKETS=4",saltedTableName);

        conn.createStatement().execute(saltedDDL);
        conn.commit();

        //If we attempt to order by the row key we should not fail
        boolean failed = false;
        String sql = "SELECT * FROM " + saltedTableName + " ORDER BY K1,K2,K3 LIMIT 2 OFFSET (k1,k2,k3)=(2, 3, 1)";
        try {
            ResultSet rs = conn.createStatement().executeQuery(sql);
        } catch(Exception e) {
            //Should fail
            failed = true;
        }
        assertFalse(failed);

        //If we attempt to order by the not row key we should fail
        failed = false;
        sql = "SELECT * FROM " + saltedTableName + " ORDER BY K2,K1,K3 LIMIT 2 OFFSET (k1,k2,k3)=(2, 3, 1)";
        try {
            ResultSet rs = conn.createStatement().executeQuery(sql);
        } catch(Exception e) {
            //Should fail
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void testSaltedTableRVCOffset() throws SQLException {
        //Make a salted table
        String saltedTableName = "T_" + generateUniqueName();

        String saltedDDL = String.format(DATA_DDL + "SALT_BUCKETS=4",saltedTableName);

        conn.createStatement().execute(saltedDDL);
        conn.commit();

        String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?)", saltedTableName);

        int nRows = 0;
        PreparedStatement ps = conn.prepareStatement(upsertDML);
        for (int k1 = 0; k1 < 4; k1++) {
            ps.setInt(1, k1);
            for (int k2 = 0; k2 < 4; k2++) {
                ps.setInt(2, k2);
                for (int k3 = 0; k3 < 4; k3++) {
                    ps.setInt(3, k3);
                    ps.setInt(4, nRows);
                    int result = ps.executeUpdate();
                    assertEquals(1, result);
                    nRows++;
                }
            }
        }
        conn.commit();

        boolean failed = false;
        String sql = "SELECT * FROM " + saltedTableName + " ORDER BY K1,K2,K3 LIMIT 3 OFFSET (k1,k2,k3)=(2, 3, 1)";

        ResultSet rs = conn.createStatement().executeQuery(sql);

        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(1, k3);
        }
        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(2, k3);
        }
        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(3, k3);
        }
        assertFalse(rs.next());

    }

    @Test
    public void testGlobalViewRVCOffset() throws SQLException {
        //Make a view
        String viewName1 = "V_" + generateUniqueName();

        //Simple View
        String viewDDL = "CREATE VIEW " + viewName1 + " AS SELECT * FROM " + dataTableName ;
        conn.createStatement().execute(viewDDL);
        conn.commit();

        String sql = "SELECT  k2,k1,k3 FROM " + viewName1 + " LIMIT 3 OFFSET (k2,k1,k3)=(3, 3, 2)";

        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        {
            int k2 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k3 = rs.getInt(3);

            assertEquals(3, k2);
            assertEquals(3, k1);
            assertEquals( 2, k3);
        }
        assertTrue(rs.next());
        {
            int k2 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k3 = rs.getInt(3);

            assertEquals(3, k2);
            assertEquals(3, k1);
            assertEquals( 3, k3);
        }
        assertTrue(rs.next());
        {
            int k2 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k3 = rs.getInt(3);

            assertEquals(2, k2);
            assertEquals(0, k1);
            assertEquals( 0, k3);
        }
        assertFalse(rs.next());
    }

    @Test
    public void testTenantRVCOffset() throws SQLException {

        String multiTenantDataTableName = "T_" + generateUniqueName();

        String multiTenantDDL = String.format("CREATE TABLE %s (tenant_id VARCHAR NOT NULL, k1 TINYINT NOT NULL,\n" + "k2 TINYINT NOT NULL,\n"
                + "k3 TINYINT NOT NULL,\n" + "v1 INTEGER,\n" + "CONSTRAINT pk PRIMARY KEY (tenant_id, k1, k2, k3)) MULTI_TENANT=true",multiTenantDataTableName);

        conn.createStatement().execute(multiTenantDDL);
        conn.commit();

        String tenantId2 = "tenant2";

        //tenant connection
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId2);
        Connection tenant2Connection = DriverManager.getConnection(getUrl(), props);

        //create tenant view with new pks
        String viewName = multiTenantDataTableName + "_" + tenantId2;
        tenant2Connection.createStatement().execute("CREATE VIEW " + viewName + " ( vk1 INTEGER NOT NULL, vv1 INTEGER, CONSTRAINT PKVIEW PRIMARY KEY(vk1))  AS SELECT * FROM " + multiTenantDataTableName);

        //create tenant view index on tenant view
        String viewIndexName = viewName+"_Index1";
        tenant2Connection.createStatement().execute("CREATE INDEX " + viewIndexName + " ON " + viewName + " ( vv1 ) ");

        String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?,?,?)", viewName);
        int tenantRows = 0;
        PreparedStatement tps = tenant2Connection.prepareStatement(upsertDML);
        for (int k1 = 0; k1 < 4; k1++) {
            tps.setInt(1, k1);
            for (int k2 = 0; k2 < 4; k2++) {
                tps.setInt(2, k2);
                for (int k3 = 0; k3 < 4; k3++) {
                    tps.setInt(3, k3);
                    tps.setInt(4, tenantRows);
                    for(int vk1 = 0; vk1 < 4; vk1++){
                        tps.setInt(5, vk1);
                        tps.setInt(6, -tenantRows); //vv1

                        int result = tps.executeUpdate();
                        assertEquals(1, result);
                        tenantRows++;
                    }
                }
            }
        }

        tenant2Connection.commit();

        //tenant view
        String sql = "SELECT * FROM " + viewName + "  LIMIT 2 OFFSET (k1,k2,k3,vk1)=(2, 3, 1, 3)";
        ResultSet rs = tenant2Connection.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            int vk1 = rs.getInt(5);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(1, k3);
            assertEquals(3, vk1);
        }
        assertTrue(rs.next());
        {
            int k1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k3 = rs.getInt(3);
            int vk1 = rs.getInt(5);
            assertEquals(2, k1);
            assertEquals(3, k2);
            assertEquals(2, k3);
            assertEquals(0, vk1);
        }
        assertFalse(rs.next());
    }

    @Test
    public void testViewIndexRVCOffset() throws SQLException {

        String multiTenantDataTableName = "T_" + generateUniqueName();

        String multiTenantDDL = String.format("CREATE TABLE %s (tenant_id VARCHAR NOT NULL, k1 TINYINT NOT NULL,\n" + "k2 TINYINT NOT NULL,\n"
                + "k3 TINYINT NOT NULL,\n" + "v1 INTEGER,\n" + "CONSTRAINT pk PRIMARY KEY (tenant_id, k1, k2, k3)) MULTI_TENANT=true",multiTenantDataTableName);

        conn.createStatement().execute(multiTenantDDL);
        conn.commit();

        String tenantId2 = "tenant2";

        //tenant connection
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId2);
        Connection tenant2Connection = DriverManager.getConnection(getUrl(), props);

        //create tenant view with new pks
        String viewName = multiTenantDataTableName + "_" + tenantId2;
        tenant2Connection.createStatement().execute("CREATE VIEW " + viewName + " ( vk1 INTEGER NOT NULL, vv1 INTEGER, CONSTRAINT PKVIEW PRIMARY KEY(vk1))  AS SELECT * FROM " + multiTenantDataTableName);

        //create tenant view index on tenant view
        String viewIndexName = viewName+"_Index1";
        tenant2Connection.createStatement().execute("CREATE INDEX " + viewIndexName + " ON " + viewName + " ( vv1 ) ");

        String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?,?,?)", viewName);
        int tenantRows = 0;
        PreparedStatement tps = tenant2Connection.prepareStatement(upsertDML);
        for (int k1 = 0; k1 < 4; k1++) {
            tps.setInt(1, k1);
            for (int k2 = 0; k2 < 4; k2++) {
                tps.setInt(2, k2);
                for (int k3 = 0; k3 < 4; k3++) {
                    tps.setInt(3, k3);
                    tps.setInt(4, tenantRows);
                    for(int vk1 = 0; vk1 < 4; vk1++){
                        tps.setInt(5, vk1);
                        tps.setInt(6, -tenantRows); //vv1

                        int result = tps.executeUpdate();
                        assertEquals(1, result);
                        tenantRows++;
                    }
                }
            }
        }

        tenant2Connection.commit();

        //View Index Queries
        String sql = "SELECT vv1,k1,k2,k3,vk1 FROM " + viewName + " ORDER BY vv1 LIMIT 3 OFFSET (vv1,k1,k2,k3,vk1)=(-196, 3,0,0,1)";
        ResultSet rs = tenant2Connection.createStatement().executeQuery(sql);

        assertTrue(rs.next());
        {
            int vv1 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k2 = rs.getInt(3);
            int k3 = rs.getInt(4);
            int vk1 = rs.getInt(5);

            assertEquals(-196, vv1);
            assertEquals(3, k1);
            assertEquals(0, k2);
            assertEquals( 1, k3);
            assertEquals(0, vk1);
        }
        assertTrue(rs.next());
        {
            int vv1 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k2 = rs.getInt(3);
            int k3 = rs.getInt(4);
            int vk1 = rs.getInt(5);

            assertEquals(-195, vv1);
            assertEquals(3, k1);
            assertEquals(0, k2);
            assertEquals( 0, k3);
            assertEquals(3, vk1);
        }
        assertTrue(rs.next());
        {
            int vv1 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k2 = rs.getInt(3);
            int k3 = rs.getInt(4);
            int vk1 = rs.getInt(5);

            assertEquals(-194, vv1);
            assertEquals(3, k1);
            assertEquals(0, k2);
            assertEquals( 0, k3);
            assertEquals(2, vk1);
        }
        assertFalse(rs.next());

    }

    @Test
    public void testIndexRVCOffset() throws SQLException {
        String sql = "SELECT  k2,k1,k3 FROM " + dataTableName + " LIMIT 3 OFFSET (k2,k1,k3)=(3, 3, 2)";

        //make sure this goes to index TODO
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        {
            int k2 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k3 = rs.getInt(3);

            assertEquals(3, k2);
            assertEquals(3, k1);
            assertEquals( 2, k3);
        }
        assertTrue(rs.next());
        {
            int k2 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k3 = rs.getInt(3);

            assertEquals(3, k2);
            assertEquals(3, k1);
            assertEquals( 3, k3);
        }
        assertTrue(rs.next());
        {
            int k2 = rs.getInt(1);
            int k1 = rs.getInt(2);
            int k3 = rs.getInt(3);

            assertEquals(2, k2);
            assertEquals(0, k1);
            assertEquals( 0, k3);
        }
        assertFalse(rs.next());
    }

    @Test
    public void testUncoveredIndexRVCOffsetFails() throws SQLException {
        //v1 is not in the index
        try {
            String sql = "SELECT  k2,k1,k3,v1 FROM " + dataTableName + " LIMIT 3 OFFSET (k2,k1,k3)=(3, 3, 2)";
            ResultSet rs = conn.createStatement().executeQuery(sql);

            sql = "EXPLAIN SELECT k2,k1,k3,v1 FROM " + dataTableName + " LIMIT 3 OFFSET (k2,k1,k3)=(3, 3, 2)";
            rs = conn.createStatement().executeQuery(sql);


        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        fail("Should not have allowed uncovered index access with RVC Offset without hinting to index.");
    }

    @Test
    public void testIndexSaltedBaseTableRVCOffset() throws SQLException {
        String saltedTableName = "T_" + generateUniqueName();

        String saltedDDL = String.format(DATA_DDL + "SALT_BUCKETS=4",saltedTableName);

        conn.createStatement().execute(saltedDDL);
        conn.commit();

        String indexName = "I_" + generateUniqueName();

        String indexDDL = String.format("CREATE INDEX %s ON %s (v1,k2)",indexName,saltedTableName);

        conn.createStatement().execute(indexDDL);
        conn.commit();

        String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?)", saltedTableName);

        int nRows = 0;
        PreparedStatement ps = conn.prepareStatement(upsertDML);
        for (int k1 = 0; k1 < 4; k1++) {
            ps.setInt(1, k1);
            for (int k2 = 0; k2 < 4; k2++) {
                ps.setInt(2, k2);
                for (int k3 = 0; k3 < 4; k3++) {
                    ps.setInt(3, k3);
                    ps.setInt(4, nRows);
                    int result = ps.executeUpdate();
                    assertEquals(1, result);
                    nRows++;
                }
            }
        }
        conn.commit();

        //Note Today Salted Base Table forces salted index
        String sql = "SELECT v1,k2,k1,k3 FROM " + saltedTableName + " LIMIT 3 OFFSET (v1,k2,k1,k3)=(9, 2, 0, 1)";
        ResultSet rs = conn.createStatement().executeQuery(sql);

        assertTrue(rs.next());
        {
            int v1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k1 = rs.getInt(3);
            int k3 = rs.getInt(4);
            assertEquals(9, v1);
            assertEquals(2, k2);
            assertEquals(0, k1);
            assertEquals(1, k3);
        }
        assertTrue(rs.next());
        {
            int v1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k1 = rs.getInt(3);
            int k3 = rs.getInt(4);
            assertEquals(10, v1);
            assertEquals(2, k2);
            assertEquals(0, k1);
            assertEquals(2, k3);
        }
        assertTrue(rs.next());
        {
            int v1 = rs.getInt(1);
            int k2 = rs.getInt(2);
            int k1 = rs.getInt(3);
            int k3 = rs.getInt(4);
            assertEquals(11, v1);
            assertEquals(2, k2);
            assertEquals(0, k1);
            assertEquals(3, k3);
        }
        assertFalse(rs.next());
    }

    @Test
    public void testIndexMultiColumnsRVCOffset() throws SQLException {


        String ddlTemplate = "CREATE TABLE %s (k1 TINYINT NOT NULL,\n" +
                "k2 TINYINT NOT NULL,\n" +
                "k3 TINYINT NOT NULL,\n" +
                "k4 TINYINT NOT NULL,\n" +
                "k5 TINYINT NOT NULL,\n" +
                "k6 TINYINT NOT NULL,\n" +
                "v1 INTEGER,\n" +
                "v2 INTEGER,\n" +
                "v3 INTEGER,\n" +
                "v4 INTEGER,\n" +
                "CONSTRAINT pk PRIMARY KEY (k1, k2, k3, k4, k5, k6)) ";

        String longKeyTableName = "T_" + generateUniqueName();
        String longKeyIndex1Name =  "INDEX_1_" + longKeyTableName;
        String longKeyIndex2Name =  "INDEX_2_" + longKeyTableName;

        String ddl = String.format(ddlTemplate,longKeyTableName);

        conn.createStatement().execute(ddl);

        String createIndex1 = "CREATE INDEX IF NOT EXISTS " + longKeyIndex1Name + " ON " + longKeyTableName + " (k2 ,v1, k4)";

        String createIndex2 = "CREATE INDEX IF NOT EXISTS " + longKeyIndex2Name + " ON " + longKeyTableName + " (v1, v3)";

        conn.createStatement().execute(createIndex1);
        conn.createStatement().execute(createIndex2);

        conn.commit();


        //Note that there is some risk in OFFSET with indexes
        //If the OFFSET is coercible to multiple indexes/base table it could mean
        // very different positions based on key
        //To Handle This the INDEX hint needs to be used to specify an index offset for safety

        String sql = "SELECT /*INDEX(" + longKeyTableName + " " + createIndex1 + ")*/ k2,v1,k4 FROM " + dataTableName + " LIMIT 3 OFFSET (3, 1, 8)";

        sql = "SELECT  k2,v1,k4 FROM " + longKeyTableName + " LIMIT 3 OFFSET (k2,v1,k4)=(3, 1, 5)";


        try {
            ResultSet rs = conn.createStatement().executeQuery(sql);
        } catch(Exception e){

        }

        sql = "SELECT  v1,v3 FROM " + longKeyTableName + " LIMIT 3 OFFSET (v1,v3)=(3, 1)";


        try {
            ResultSet rs = conn.createStatement().executeQuery(sql);
        } catch(Exception e){

        }


    }

    @Test
    public void testOffsetExplain() throws SQLException {
        String sql = "EXPLAIN SELECT * FROM " + dataTableName + "  LIMIT 2 OFFSET (k1,k2,k3)=(2, 3, 2)";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        StringBuilder explainStringBuilder = new StringBuilder();
        while(rs.next())
        {
            String explain = rs.getString(1);
            explainStringBuilder.append(explain);
        }
        assertTrue(explainStringBuilder.toString().contains("With RVC Offset"));
    }


}
