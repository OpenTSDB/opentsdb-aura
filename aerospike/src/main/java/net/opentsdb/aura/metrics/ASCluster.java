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

package net.opentsdb.aura.metrics;

import com.aerospike.client.Host;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.base.Strings;

/**
 * One per app (or cluster). Then we create as many clients as we want. But this
 * handles the actual communication and record locating.
 */
public class ASCluster {

    private final Cluster cluster;
    private final Host[] hosts;

    public ASCluster(final String hostList,
                     final int port) {
        // TODO - we'll need these eventually
        final ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = null;
        clientPolicy.password = null;
        clientPolicy.tlsPolicy = null;

        if (Strings.isNullOrEmpty(hostList)) {
            throw new IllegalArgumentException("Unable to start Aerospike cluster client without a host list.");
        }
        this.hosts = Host.parseHosts(hostList, port);
        cluster = new Cluster(clientPolicy, hosts);
    }

    public Cluster cluster() {
        return cluster;
    }

    public Host[] getHosts() {
        return hosts;
    }

}
