package org.apache.phoenix.query;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HCONNECTIONS_COUNTER;

public class TransientConnectionStatsTableProvider implements StatsTableProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            TransientConnectionStatsTableProvider.class);
    private ReadOnlyProps readOnlyProps = null;
    private Configuration config = null;

    TransientConnectionStatsTableProvider(ReadOnlyProps readOnlyProps, Configuration config){
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
        Connection connection;
        try {
            connection = HBaseFactoryProvider.getHConnectionFactory().createConnection(this.config);
            GLOBAL_HCONNECTIONS_COUNTER.increment();
            LOGGER.info("HConnection established. Stacktrace for informational purposes: " + connection + " "
                    + LogUtil.getCallerStackTrace());
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION).setRootCause(e).build()
                    .buildException();
        }
        if (connection.isClosed()) { // TODO: why the heck doesn't this throw above?
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION).build().buildException();
        }
        return Optional.of(connection);
    }

    @Override
    public Table getTable(Optional<Connection> connection) throws SQLException {
        Preconditions.checkState(connection.isPresent());
        TableName tableName = SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES,
                readOnlyProps);
        byte[] tableBytes = tableName.getName();

        try {
            return HBaseFactoryProvider.getHTableFactory().getTable(tableBytes, connection.get(), null);
        } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            throw new org.apache.phoenix.schema.TableNotFoundException(SchemaUtil.getSchemaNameFromFullName(tableBytes),
                    SchemaUtil.getTableNameFromFullName(tableBytes));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }
}