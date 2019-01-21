package org.apache.phoenix.iterate;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.SQLException;
import java.util.List;
import java.util.TreeSet;

/**
 * class to query for raw full/partial client statistics
 * May generalize in the future for non v1 guideposts
 */
public class PhoenixClientStatisticsLoader {

    private final ConnectionQueryServices connectionQueryServices;

    public PhoenixClientStatisticsLoader(final ConnectionQueryServices connectionQueryServices){
        this.connectionQueryServices = connectionQueryServices;
    }

    byte[] buildColumnFamiliesForGuidePostsKey(final PTable pTable, final List<Pair<byte[], byte[]>> whereConditionColumns){
        TreeSet<byte[]> whereConditions = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for(Pair<byte[], byte[]> where : whereConditionColumns) {
            byte[] cf = where.getFirst();
            if (cf != null) {
                whereConditions.add(cf);
            }
        }

        byte[] defaultCF = SchemaUtil.getEmptyColumnFamily(pTable);
        byte[] cf = null;
        if ( !pTable.getColumnFamilies().isEmpty() && !whereConditions.isEmpty() ) {
            for(Pair<byte[], byte[]> where : whereConditionColumns) {
                byte[] whereCF = where.getFirst();
                if (Bytes.compareTo(defaultCF, whereCF) == 0) {
                    cf = defaultCF;
                    break;
                }
            }
            if (cf == null) {
                cf = whereConditionColumns.get(0).getFirst();
            }
        }
        if (cf == null) {
            cf = defaultCF;
        }
        return cf;
    }

    public GuidePostsInfo getGuidePostInfo(final PTable pTable, final List<Pair<byte[], byte[]>> whereConditionColumns) throws SQLException {

        byte[] cf = buildColumnFamiliesForGuidePostsKey(pTable, whereConditionColumns);

        GuidePostsKey key = new GuidePostsKey(pTable.getPhysicalName().getBytes(), cf);

        return connectionQueryServices.getTableStats(key);
    }
}
