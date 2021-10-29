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

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DeploymentConfig {

    private final String clusterDomain;

    private final Map<String,Namespace> namespaceMap;

    public DeploymentConfig(String clusterDomain, Map<String,Namespace> namespaceMap){
        this.clusterDomain = clusterDomain;
        this.namespaceMap = namespaceMap;
    }

    public String getClusterDomain() {
        return clusterDomain;
    }

    public Namespace getNamespace(String namespace) {
        return namespaceMap.get(namespace);
    }

    public static class Builder {

        private String l_clusterDomain;
        private String l_defaultNamespace;
        private Namespace[] namespaces;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withClusterDomain(String clusterDomain) {
            this.l_clusterDomain = clusterDomain;
            return this;
        }

        public Builder withDefaultNamespace(String defaultNamespace) {
            this.l_defaultNamespace = l_defaultNamespace;
            return this;
        }

        public Builder withNamespaces(Namespace[] namespaces) {
            this.namespaces = namespaces;
            return this;
        }



        public DeploymentConfig build() {

            final Optional<Namespace> defaultNamespace = Arrays.stream(namespaces)
                    .filter(namespace -> namespace.getName().equals(l_defaultNamespace))
                    .findFirst();

            final Map<String, Namespace> namespaceMap = Arrays.stream(namespaces)
                    .map(namespace -> {
                        namespace.toMap();
                        namespace.setDefaultNamespace(defaultNamespace.orElse(null));
                        return namespace;
                    })
                    .collect(Collectors.toMap(Namespace::getName, Function.identity()));

            return new DeploymentConfig(l_clusterDomain, namespaceMap);

        }

    }

}
