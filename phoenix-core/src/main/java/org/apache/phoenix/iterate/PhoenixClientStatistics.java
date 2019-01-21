package org.apache.phoenix.iterate;

import org.apache.phoenix.query.KeyRange;

public interface PhoenixClientStatistics {

    //provides 4 main things
    //row/cardinality estimate
    //byte estimate
    //rowkey offsets
    //time of its estimates

    //region agnostic api
    //As of writing this is only used for guidepost based statistics may use different approach in the future
    //Should not have any info on how to load the stats.


    public boolean hasStatistics();

    //Returns Bins that cover the key range.
    public PhoenixClientStatisticsSummary getStatisticsBins(KeyRange range);
}
