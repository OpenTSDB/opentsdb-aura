/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  Yahoo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.aura.metrics.core;

import java.util.Iterator;

public interface LongTermStorage {

    boolean flush(long hash, TimeSeriesEncoder encoder);

    /**
     *
     * @param hash
     * @param startTimestamp
     * @param endTimestamp The EXCLUSIVE end time!!! Important
     * @return
     */
    Records read(long hash, int startTimestamp, int endTimestamp);

    /**
     * This sucker works like this:
     * Call {@link #hasNext()} to see if there is any data to read. While true
     * call {@link #next()} to get the actual segment. Note that
     * the segments can be out of order and there may be gaps in between.
     * NOTE That a call to next may trigger another fetch to storage.
     */
    public interface Records extends Iterator<TimeSeriesEncoder> {
    }
}