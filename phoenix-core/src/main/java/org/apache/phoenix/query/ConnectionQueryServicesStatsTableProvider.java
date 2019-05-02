package org.apache.phoenix.query;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HCONNECTIONS_COUNTER;

import java.io.IOException;
import java.sql.SQLException;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionQueryServicesStatsTableProvider implements StatsTableProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            ConnectionQueryServicesStatsTableProvider.class);
    private final ReadOnlyProps readOnlyProps;
    private final Configuration config;
    private final ConnectionQueryServices queryServices;

    ConnectionQueryServicesStatsTableProvider(ConnectionQueryServices queryServices, ReadOnlyProps readOnlyProps, Configuration config){
        this.queryServices = queryServices;
        this.readOnlyProps = readOnlyProps;
        this.config = config;
    }

    @Override
    public ReadOnlyProps getReadOnlyProps() {
        return readOnlyProps;
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public Optional<Connection> openConnection() throws SQLException {
        //Connection is maintained in queryServices
        return Optional.absent();
    }

    @SuppressWarnings("unused")
    @Override
    public Table getTable(Optional<Connection> connection) throws SQLException {
        TableName tableName = SchemaUtil.getPhysicalName(
                PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES,
                queryServices.getProps());
        return queryServices.getTable(tableName.getName());
    }
}