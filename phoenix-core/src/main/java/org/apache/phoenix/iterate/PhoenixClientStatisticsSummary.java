package org.apache.phoenix.iterate;

import java.util.List;
import java.util.Objects;


//pojo
public class PhoenixClientStatisticsSummary {

    private final long sizeBytes;
    private final long numRows;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final List<PhoenixClientStatisticsBin> bins;

    public PhoenixClientStatisticsSummary(long sizeBytes, long numRows, long minTimestamp,
            long maxTimestamp, List<PhoenixClientStatisticsBin> bins) {
        this.sizeBytes = sizeBytes;
        this.numRows = numRows;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.bins = bins;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PhoenixClientStatisticsSummary that = (PhoenixClientStatisticsSummary) o;
        return sizeBytes == that.sizeBytes && numRows == that.numRows
                && minTimestamp == that.minTimestamp && maxTimestamp == that.maxTimestamp && Objects
                .equals(bins, that.bins);
    }

    @Override public int hashCode() {
        return Objects.hash(sizeBytes, numRows, minTimestamp, maxTimestamp, bins);
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public long getNumRows() {
        return numRows;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public List<PhoenixClientStatisticsBin> getBins() {
        return bins;
    }
}
