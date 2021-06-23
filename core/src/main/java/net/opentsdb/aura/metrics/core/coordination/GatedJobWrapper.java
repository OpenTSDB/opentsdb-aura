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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Use only this class to manage gated jobs.
 */
public class GatedJobWrapper implements JobWrapper {
    private final Job previousJob;
    private Job currentJob;
    private final Clock clock;
    private final Gate gate;
    private final Gate.KEY key;
    private final Function<Runnable, Boolean> runnableConsumer;
    private static final Logger LOGGER = LoggerFactory.getLogger(GatedJobWrapper.class);
    private volatile boolean jobSubmitted = false;

    public GatedJobWrapper(Job previousJob,
                      Job currentJob,
                      Clock clock,
                      Gate gate,
                      Gate.KEY key,
                           Function<Runnable, Boolean> runnableConsumer) {
        this.previousJob = previousJob;
        this.currentJob = currentJob;
        this.clock = clock;
        this.gate = gate;
        this.key = key;
        this.runnableConsumer = runnableConsumer;
    }

    @Override
    public boolean tryToRun() {
        if(previousJob != null) {
            if(previousJob.isComplete()) {
                //Job is done, call close
                previousJob.close();
            } else {
                LOGGER.info("Previous job {} is still running, wont submit a new one", previousJob);
                //Previous Job is still running.
                return false;
            }
        }

        if (jobSubmitted) {
            //Job already submitted
            return false;
        }

        //Close gate incase it was
        //opened before and never closed.
        closeGate();

        if(clock.isTimeToRun()) {
            if(openGate()) {
                if(runnableConsumer.apply(currentJob)) {
                    clock.advance();
                    this.jobSubmitted = true;
                    //Should no longer try to submit this job
                    return true;
                } else {
                    closeGate();
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public JobWrapper createNext() {
        return new GatedJobWrapper(
                currentJob,
                currentJob.createNext(),
                clock,
                gate,
                key,
                runnableConsumer);
    }

    protected void closeGate() {
        LOGGER.info("Closing gate for key: {} current {} for job: {}", key, gate, currentJob);
        boolean close = gate.close(key.ordinal());
        LOGGER.info("Closed gate for key: {} current {} for job: {} status: {}", key, gate, currentJob, close);
    }

    protected boolean openGate() {
        boolean state = gate.open(key.ordinal());
        LOGGER.info("Opening gate {} with key: {} current gate state: {} for job: {}",
                (state ? "success": "failure"), key, gate, currentJob);
        return gate.open(key.ordinal());
    }

    @Override
    public String toString() {
        return String.format("Type: [%s] Time: [%s] Previous Job: [%s] Current Job: [%s]", key.name(), clock, previousJob, currentJob);
    }

}