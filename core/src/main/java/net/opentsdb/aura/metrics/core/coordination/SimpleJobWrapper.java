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

import java.util.function.Consumer;

/**
 * This wrapper is to facilitate
 * running of a single job
 */
public class SimpleJobWrapper implements JobWrapper {

    private final Job job;
    private final Consumer<Runnable> runnableConsumer;

    public SimpleJobWrapper(Job job,
            Consumer<Runnable> runnableConsumer) {

        this.job = job;
        this.runnableConsumer = runnableConsumer;
    }

    @Override
    public boolean tryToRun() {
        runnableConsumer.accept(job);
        return true;
    }

    @Override
    public JobWrapper createNext() {
        return null;
    }
}
