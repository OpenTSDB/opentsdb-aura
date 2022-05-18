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

package net.opentsdb.aura.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import net.opentsdb.aura.execution.AuraMetricsClusterConfig.Host;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class AuraMetricsClusterConfigTest {

  @Test
  public void ignoreUnknownField() {
    ObjectNode hostNode = JsonNodeFactory.instance.objectNode();
    hostNode.set("host", TextNode.valueOf("test.host.1"));
    hostNode.set("uri", TextNode.valueOf("test.host.1"));
    hostNode.set(
        "namespaces",
        new ArrayNode(JsonNodeFactory.instance).add(TextNode.valueOf("test_namespace")));
    hostNode.set("retention", TextNode.valueOf("12h"));
    hostNode.set("unknown_field", TextNode.valueOf("should be ignored"));

    Host host = new ObjectMapper().convertValue(hostNode, Host.class);
    assertEquals(hostNode.get("host").asText(), host.host);
    assertEquals(hostNode.get("uri").asText(), host.uri);
    assertEquals(hostNode.get("retention").asText(), host.retention);
    JsonNode namespaces = hostNode.get("namespaces");
    assertEquals(1, namespaces.size());
    assertEquals(namespaces.get(0).asText(), host.namespaces.get(0));
  }
}
