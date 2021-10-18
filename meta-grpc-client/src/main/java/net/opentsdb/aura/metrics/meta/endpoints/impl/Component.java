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
package net.opentsdb.aura.metrics.meta.endpoints.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Component {

    @JsonProperty("component")
    private String name;

    @JsonProperty("num-shards")
    private int numShards;

    @JsonProperty("replicas")
    private int numReplicas;

    private static final String[] replicas = {"a", "b", "c", "d", "e"};
    private int port;


    public Component() {

    }

    public String getName() {

        return name;
    }

    public int getNumShards() {

        return numShards;
    }

    public int getNumReplicas() {
        if(numReplicas < replicas.length) {
            return numReplicas;
        }else{
            return 1;
        }
    }

    public void setNumReplicas(int numReplicas) {
        if(numReplicas < replicas.length) {
            this.numReplicas = numReplicas;
        }else{
            this.numReplicas = 1;
        }
    }

    public String getReplica(int replicaId){
        return replicas[replicaId];
    }

    public int getPort() {

        return port;
    }

    public void setPort(int port) {

        this.port = port;
    }


    public void setNumShards(int numShards) {

        this.numShards = numShards;
    }

    public void setName(String name) {

        this.name = name;
    }

    public void setDefaultComponent(Component defaultComponent) {
        if (this.numShards == 0){
            this.numShards = defaultComponent.getNumShards();
        }
        if (this.numReplicas == 0){
            this.numReplicas = defaultComponent.getNumReplicas();
        }
        if (this.port == 0){
            this.port = defaultComponent.getPort();
        }
    }

    @Override
    public String toString() {
        return "Components {name=" + name + ", num-shards=" + numShards +  ", replicas=" + numReplicas + ", port=" + port + "}";
    }
}
