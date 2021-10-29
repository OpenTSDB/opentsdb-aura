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

package net.opentsdb.aura.metrics.meta.endpoints;

import net.opentsdb.aura.metrics.meta.endpoints.impl.Component;

public class MystStatefulSetRegistry extends BaseStatefulSetRegistry {


    private static final String TYPE = "MystStatefulSetRegistry";
    private static final String COMPONENT_NAME = "myst";
    private static final String pod_name_prefix = "%s-myst-%s";

    @Override
    protected String getComponentName() {
        return COMPONENT_NAME;
    }

    @Override
    protected String getPrefix(String namespace, Component component, String replica, long epoch) {
        return String.format(pod_name_prefix, namespace, replica);
    }

    @Override
    public String type() {
        return TYPE;
    }
}
