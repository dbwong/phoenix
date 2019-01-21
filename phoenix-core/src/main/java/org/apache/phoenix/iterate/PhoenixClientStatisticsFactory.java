package org.apache.phoenix.iterate;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;

import java.sql.SQLException;
import java.util.List;

public class PhoenixClientStatisticsFactory {

    private final PhoenixClientStatisticsLoader phoenixClientStatisticsLoader;

    PhoenixClientStatisticsFactory(PhoenixClientStatisticsLoader phoenixClientStatisticsLoader){
        this.phoenixClientStatisticsLoader = phoenixClientStatisticsLoader;
    }

    PhoenixClientStatistics getPhoenixClientStatistics(PTable pTable, List<Pair<byte[], byte[]>>
            whereConditionColumns) throws SQLException {

        //Load the statistics into our stats specific data structure
        //for now all statistics will be assumed to be the v1 guidepostinfo
        GuidePostsInfo gpi = phoenixClientStatisticsLoader.getGuidePostInfo(pTable, whereConditionColumns);


        PhoenixClientStatistics statistics = new GuidePostPhoenixClientStatistics(gpi);

        return statistics;
    }
}
