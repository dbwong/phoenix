/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.query;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.base.Optional;

public class TransientConnectionStatsTableProviderTest {

    TransientConnectionStatsTableProvider provider;

    @Mock
    HConnectionFactory hConnectionFactory;

    @Mock
    HTableFactory hTableFactory;

    @Mock
    ReadOnlyProps readOnlyProps;

    @Mock
    Configuration config;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        provider = new TransientConnectionStatsTableProvider(hConnectionFactory, hTableFactory, readOnlyProps, config);
    }

    @Test
    public void constructorTest() {
        assertEquals(config, provider.getConfiguration());
        assertEquals(readOnlyProps, provider.getReadOnlyProps());
    }

    @Test
    public void openConnectionTest() throws Exception {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(hConnectionFactory.createConnection(Mockito.any())).thenReturn(mockConnection);

        Optional<Connection> result = provider.openConnection();
        assertEquals(result.get(), mockConnection);
    }

    @Test(expected = SQLException.class)
    public void openConnectionExceptionTest() throws Exception {
        Mockito.when(hConnectionFactory.createConnection(Mockito.any())).thenThrow(new IOException());

        Optional<Connection> result = provider.openConnection();
    }

    @Test
    public void getTableTest() throws Exception {
        Connection mockConnection = Mockito.mock(Connection.class);
        Optional<Connection> connection = Optional.of(mockConnection);

        Table mockTable = Mockito.mock(Table.class);

        Mockito.when(hTableFactory.getTable(Mockito.any(), Mockito.eq(mockConnection), Mockito.eq(null)))
                .thenReturn(mockTable);

        Table result = provider.getTable(connection);

        assertEquals(mockTable, result);
    }

    @Test(expected = Exception.class)
    public void getTableTestBadConnectionTest() throws Exception {
        Optional<Connection> connection = Optional.absent();
        provider.getTable(connection);
    }

}