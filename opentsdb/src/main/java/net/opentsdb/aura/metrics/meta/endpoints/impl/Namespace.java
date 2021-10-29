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

import java.util.*;

public class Namespace {

    @JsonProperty("namespace")
    private String name;

    @JsonProperty("k8s_namespace")
    private String k8sNamespace;

    private  List<Component> components;
    private final Map<String,Component> componentMap = new HashMap<>();

    public Namespace(){

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
         this.name = name;
    }

    public void setDefaultNamespace(Namespace defaultNamespace) {

        if (Objects.nonNull(defaultNamespace)) {
            if (Objects.isNull(this.k8sNamespace)) {
                this.k8sNamespace = defaultNamespace.getk8sNamespace();
            }
            final Map<String, Component> defaultComponentMap = defaultNamespace.componentMap;
            Component defaultComponent = null;
            for (String key : defaultComponentMap.keySet()) {
                defaultComponent = defaultComponentMap.get(key);
                if (componentMap.containsKey(key)) {
                    componentMap.get(key).setDefaultComponent(defaultComponent);
                } else {
                    componentMap.put(defaultComponent.getName(), defaultComponent);
                }
            }
        }
    }

    public String getk8sNamespace() {
        return k8sNamespace;
    }

    public void setk8sNamespace(String k8sNamespace) {
        this.k8sNamespace = k8sNamespace;
    }
    
    public List<Component> getComponents(){
        return components;
    }

    public void setComponents(List<Component> components){
        this.components = components;
    }

    public Component getComponent(String name) {
        return componentMap.get(name);
    }

    public Map<String,Component> toMap()
    {
        if (components != null) {
            for (Component component : components) {
                componentMap.put(component.getName(), component);
            }
        }
        return componentMap;
    }

    @Override
    public String toString() {
        return "Namespaces {name=" + name + ", k8s_namespace=" + k8sNamespace + ", " + componentMap + "}";
    }





}
