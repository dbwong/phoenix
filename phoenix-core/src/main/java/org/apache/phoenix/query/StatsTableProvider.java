package org.apache.phoenix.query;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.util.ReadOnlyProps;

import java.sql.SQLException;

public interface StatsTableProvider {
    ReadOnlyProps getReadOnlyProps();

    Configuration getConfiguration();

    Optional<Connection> openConnection() throws SQLException;

    Table getTable(Optional<Connection> connection) throws SQLException;
}
