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

package net.opentsdb.aura.metrics.core.coordination;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;

/**
 * Doesnt adhere to wall clock epochs.
 */
public class Clock {

    private long lastRunTimeStamp;
    private final int frequencyInSeconds;
    private final SimpleDateFormat format = new SimpleDateFormat();

    public Clock(long intialDelayInSeconds, int frequencyInSeconds) {
        this.lastRunTimeStamp = (System.currentTimeMillis() / 1000) - frequencyInSeconds + intialDelayInSeconds;
        this.frequencyInSeconds = frequencyInSeconds;
    }

    public boolean isTimeToRun() {
        long currTimeInSecs = (System.currentTimeMillis() / 1000);
        return (currTimeInSecs - this.lastRunTimeStamp) > this.frequencyInSeconds;
    }

    public void advance() {
        long currTimeInSecs = System.currentTimeMillis() / 1000;
        this.lastRunTimeStamp = currTimeInSecs;
    }

    @Override
    public String toString() {
        return "Is time to run: "+ isTimeToRun() +
               " Last run time: " + format.format(Date.from(Instant.ofEpochSecond(lastRunTimeStamp))) +
               " Next run time: " + format.format(Date.from(Instant.ofEpochSecond(lastRunTimeStamp + frequencyInSeconds))) +
               " frequency: " + frequencyInSeconds;
    }
}