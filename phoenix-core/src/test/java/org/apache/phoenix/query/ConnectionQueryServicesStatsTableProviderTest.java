/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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

public class ConnectionQueryServicesStatsTableProviderTest {

    ConnectionQueryServicesStatsTableProvider provider;

    @Mock
    ConnectionQueryServices queryServices;

    @Mock
    ReadOnlyProps readOnlyProps;

    @Mock
    Configuration config;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        provider = new ConnectionQueryServicesStatsTableProvider(queryServices,readOnlyProps,config);
    }

    @Test
    public void constructorTest() {
        assertEquals(config,provider.getConfiguration());
        assertEquals(readOnlyProps,provider.getReadOnlyProps());
    }

    @Test
    public void openConnectionTest() throws Exception {
        Optional<Connection>
                result =
                provider.openConnection();
        assertFalse(result.isPresent());
    }

    @Test
    public void getTableTest() throws Exception {
        Optional<Connection> connection = Mockito.mock(Optional.class);

        Table mockTable = Mockito.mock(Table.class);

        Mockito.when(queryServices.getProps()).thenReturn( ReadOnlyProps.EMPTY_PROPS);
        Mockito.when(queryServices.getTable(Mockito.any())).thenReturn(mockTable);

        Table result = provider.getTable(connection);

        Mockito.verifyZeroInteractions(connection);
        assertEquals(mockTable,result);
    }

}