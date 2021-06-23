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

import java.util.concurrent.atomic.AtomicInteger;

public class Gate {
    public enum KEY {
        FREE,PURGE,FLUSH;
    }
    public final int FREE = KEY.FREE.ordinal();
    private AtomicInteger gate = new AtomicInteger(FREE);

    /**
     * Acquire only when things are free.
     * @param newState
     * @return
     */
    public boolean open(int newState) {
        if(gate.get() == newState) {
            //Already in state, so return true.
            //Nothing to do here.
            return true;
        }
        if(gate.get() != FREE) {
            //Some other state is holding on.
            //Not possible to acquire at this time.
            return false;
        } else {
            //State is free, so try and acquire.
            return gate.compareAndSet(FREE, newState);
        }
    }

    /**
     * Only states who have acquired can release themselves.
     * The compare and set is protection from calls of bogus release.
     * In practice, Gate should never require a lock to release.
     * @param curState
     */
    public boolean close(int curState) {
        return gate.compareAndSet(curState, FREE);
    }

    @Override
    public String toString() {
        return "Gate current state: " + KEY.values()[gate.get()].name();
    }
}