/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.iterate;

import org.apache.phoenix.query.KeyRange;

import java.util.Arrays;
import java.util.Objects;

//pojo
public class PhoenixClientStatisticsBin {
    private final boolean present;
    private final Long estimatedRows;
    private final Long estimatedSize;
    private final Long estimatedUpdateTimestamp;
    private final KeyRange binKeyRange;

    public PhoenixClientStatisticsBin(boolean present, Long estimatedRows, Long estimatedSize,
            Long estimatedUpdateTimestamp, KeyRange binKeyRange) {
        this.present = present;
        this.estimatedRows = estimatedRows;
        this.estimatedSize = estimatedSize;
        this.estimatedUpdateTimestamp = estimatedUpdateTimestamp;
        this.binKeyRange = binKeyRange;
    }

    public Long getEstimatedRows() {
        return estimatedRows;
    }

    public Long getEstimatedSize() {
        return estimatedSize;
    }

    public Long getEstimatedUpdateTimestamp() {
        return estimatedUpdateTimestamp;
    }

    public KeyRange getBinKeyRange() {
        return binKeyRange;
    }

    public boolean isPresent() {
        return present;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PhoenixClientStatisticsBin that = (PhoenixClientStatisticsBin) o;
        return present == that.present && Objects.equals(estimatedRows, that.estimatedRows)
                && Objects.equals(estimatedSize, that.estimatedSize) && Objects
                .equals(estimatedUpdateTimestamp, that.estimatedUpdateTimestamp) && Objects
                .equals(binKeyRange, that.binKeyRange);
    }

    @Override public int hashCode() {
        return Objects
                .hash(present, estimatedRows, estimatedSize, estimatedUpdateTimestamp, binKeyRange);
    }
}
