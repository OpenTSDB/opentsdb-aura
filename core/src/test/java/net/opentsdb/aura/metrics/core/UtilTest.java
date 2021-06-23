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

package net.opentsdb.aura.metrics.core;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UtilTest {

  @Test
  void serializeDeserializeTags() {
    Map<String, String> tags =
        new HashMap() {
          {
            put("host", "host1");
            put("colo", "colo1");
          }
        };
    byte[] tagBytes = Util.serializeTags(tags);
    Map<String, String> tagMap = Util.createTagMap(tagBytes);

    assertEquals(2, tagMap.size());
    assertEquals("host1", tagMap.get("host"));
    assertEquals("colo1", tagMap.get("colo"));
  }
}
