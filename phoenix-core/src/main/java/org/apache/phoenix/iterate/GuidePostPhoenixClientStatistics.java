package org.apache.phoenix.iterate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.PrefixByteCodec;
import org.apache.phoenix.util.PrefixByteDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;

/**
 * Note: There are several performance improvements from a memory usage view possible
 */
public class GuidePostPhoenixClientStatistics implements PhoenixClientStatistics {

    public static final Logger logger = LoggerFactory.getLogger(GuidePostPhoenixClientStatistics.class);

    private final GuidePostsInfo gps;

    public GuidePostPhoenixClientStatistics(GuidePostsInfo gps) {
        this.gps = gps;
    }

    @Override public boolean hasStatistics() {
        return (gps != GuidePostsInfo.NO_GUIDEPOST);
    }

    @Override
    public PhoenixClientStatisticsSummary getStatisticsBins(KeyRange keyRange) {
        long sizeBytes = 0;
        long numRows = 0;
        long minTimestamp = 0;
        long maxTimestamp = 0;
        List<PhoenixClientStatisticsBin> bins = new ArrayList<>();
        ImmutableBytesWritable startKey = new ImmutableBytesWritable(keyRange.getLowerRange());
        ImmutableBytesWritable stopKey = new ImmutableBytesWritable(keyRange.getUpperRange());

        if (gps.getGuidePostsCount() > 0) {

            ByteArrayInputStream stream = null;
            try {

                ImmutableBytesWritable guidePosts = gps.getGuidePosts();
                stream =
                        new ByteArrayInputStream(guidePosts.get(), guidePosts.getOffset(),
                                guidePosts.getLength());
                DataInput input = new DataInputStream(stream);
                PrefixByteDecoder decoder = new PrefixByteDecoder(gps.getMaxLength());

                boolean foundFirstGuidePost = false;
                ImmutableBytesWritable currentGuidePost;
                byte[] lastBinEndKey = keyRange.getLowerRange();
                for (int readGuidePosts = 0; readGuidePosts < gps.getGuidePostsCount(); ) {

                    try {
                        currentGuidePost = PrefixByteCodec.decode(decoder, input);
                        readGuidePosts++;
                    } catch (EOFException e) {
                        //no additional guide posts, should never hit
                        logger.warn("Failed to decode all the guide posts unexpected EOF occurred.",e);
                        break;
                    }

                    // Continue walking guideposts until we get past the currentKey
                    // note that even if this is greater than endKey this should still contain relevant values
                    if (!foundFirstGuidePost &&  (keyRange.lowerUnbound()  || startKey.compareTo(currentGuidePost) <= 0)) {
                        foundFirstGuidePost = true;
                    }

                    //update the time estimates since for example querying a range we dont' have guideposts for should reflect the stats of that time
                    long estimatedTimestamp = gps.getGuidePostTimestamps()[readGuidePosts - 1];
                    minTimestamp = Math.min(minTimestamp, estimatedTimestamp);
                    maxTimestamp = Math.max(maxTimestamp, estimatedTimestamp);

                    if (foundFirstGuidePost) {
                        long estimatedRows = gps.getRowCounts()[readGuidePosts - 1];
                        numRows += estimatedRows;

                        long estimatedSize = gps.getByteCounts()[readGuidePosts - 1];
                        sizeBytes += estimatedSize;

                        byte[] binEndKey = currentGuidePost.copyBytes();

                        boolean startKeyInclusive = lastBinEndKey.length > 0 ? false : true;
                        if(bins.isEmpty()){  //Inherit the first bound from the range
                            startKeyInclusive = keyRange.isLowerInclusive();
                        }

                        KeyRange binKeyRange = KeyRange.getKeyRange(lastBinEndKey,startKeyInclusive,binEndKey,true);
                        //currentGuidePost
                        PhoenixClientStatisticsBin
                                bin =
                                new PhoenixClientStatisticsBin(true,estimatedRows, estimatedSize,
                                        estimatedTimestamp, binKeyRange);
                        bins.add(bin);
                        lastBinEndKey = binEndKey;
                    }

                    //Believe it has to be strictly greater as you could have 2 guide posts (or more)
                    if (stopKey.getLength() > 0 && stopKey.compareTo(currentGuidePost) < 0) {
                        break;
                    }
                }

                //May need to make another empty bin here to represent missing GuidePosts
                if(lastBinEndKey.length != 0 && (keyRange.upperUnbound() || (stopKey.compareTo(lastBinEndKey) > 1))){
                    KeyRange binKeyRange = KeyRange.getKeyRange(lastBinEndKey,false,keyRange.getUpperRange(),keyRange.isUpperInclusive());
                    //currentGuidePost
                    PhoenixClientStatisticsBin
                            bin =
                            new PhoenixClientStatisticsBin(false, Long.valueOf(0), Long.valueOf(0),
                                    Long.valueOf(0), binKeyRange);
                    bins.add(bin);
                }

            } finally {
                if (stream != null) {
                    Closeables.closeQuietly(stream);
                }
            }

        }

        PhoenixClientStatisticsSummary
                summary =
                new PhoenixClientStatisticsSummary(sizeBytes, numRows, minTimestamp, maxTimestamp,
                        bins);

        return summary;
    }
}
