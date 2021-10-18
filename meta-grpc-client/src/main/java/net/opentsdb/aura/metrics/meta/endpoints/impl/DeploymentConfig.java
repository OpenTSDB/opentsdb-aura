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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeploymentConfig {

    @JsonProperty("kubernetes.statefulset.registry.cluster")
    private String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    @JsonProperty("kubernetes.statefulset.registry.cluster.domain")
    private String clusterDomain;

    @JsonProperty("kubernetes.statefulset.registry.namespaces")
    private List<Namespace> namespaces;

    private final Map<String,Namespace> namespaceMap = new HashMap<>();

    public DeploymentConfig(){

    }

    public String getClusterDomain() {
        return clusterDomain;
    }

    public void setClusterDomain(String clusterDomain) {
        this.clusterDomain=clusterDomain;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<Namespace> getListOfNamespaces() {
        return namespaces;
    }

    public void setListOfNamespaces(List<Namespace> namespaces) {
        this.namespaces = namespaces;
    }

    public Map<String,Namespace> toMap() {
        Namespace defaultNamespace = null;
        try {
            for (Namespace namespace : namespaces) {
                namespace.toMap();
                if (namespace.getName().equals("default")) {
                    defaultNamespace = namespace;
                } else {
                    namespaceMap.put(namespace.getName(), namespace);
                }
            }
            for (String key : namespaceMap.keySet()) {
                namespaceMap.get(key).setDefaultNamespace(defaultNamespace);
            }
        } catch (Exception e){
            System.out.println(e);
        }

        return namespaceMap;
    }

}
