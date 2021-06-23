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

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.HOURS;

public class Util {

  public static final int SECONDS_IN_AN_HOUR = (int) TimeUnit.HOURS.toSeconds(1);
  public static final int SECONDS_IN_A_DAY = (int) TimeUnit.HOURS.toSeconds(24);

  public static int getCurrentWallClockHour() {
    return getWallClockHour(System.currentTimeMillis() / 1000);
  }

  public static int getWallClockHour(long timeInSeconds) {
    return (int) (timeInSeconds - (timeInSeconds % SECONDS_IN_AN_HOUR));
  }

  public static byte[] serializeTags(Map<String, String> tags) {
    TreeMap<String, String> sortedTags = new TreeMap<>(tags);
    int tagCount = sortedTags.size();
    byte[][] tagNames = new byte[tagCount][];
    byte[][] tagValues = new byte[tagCount][];

    int i = 0;
    for (Entry<String, String> entry : tags.entrySet()) {
      String tagKey = entry.getKey();
      String tagValue = entry.getValue();
      tagNames[i] = tagKey.getBytes(StandardCharsets.UTF_8);
      tagValues[i] = tagValue.getBytes(StandardCharsets.UTF_8);
      i++;
    }
    return serializeTags(tagNames, tagValues);
  }

  public static byte[] serializeTags(byte[][] tagNames, byte[][] tagValues) {
    int tagLen = tagNames.length * 2; // separator chars
    for (int i = 0; i < tagNames.length; i++) {
      tagLen += tagNames[i].length;
      tagLen += tagValues[i].length;
    }

    byte[] tagString = new byte[tagLen];

    for (int i = 0, index = 0; i < tagNames.length; i++) {
      System.arraycopy(tagNames[i], 0, tagString, index, tagNames[i].length);
      index += tagNames[i].length;
      tagString[index++] = '\0'; // ugly null separated!

      System.arraycopy(tagValues[i], 0, tagString, index, tagValues[i].length);
      index += tagValues[i].length;
      tagString[index++] = '\0'; // ugly null separated!
    }
    return tagString;
  }

  public static Map<String, String> createTagMap(byte[] tagData) {
    return createTagMap(tagData, 0, tagData.length);
  }

  public static Map<String, String> createTagMap(byte[] tagBuffer, int offset, int length) {
    Map<String, String> tagMap = new HashMap<>();
    parseTags(tagBuffer, offset, length, (key, value) -> {
      tagMap.put(key, value);
    });
    return tagMap;
  }

  public static void parseTags(byte[] tagBuffer, int offset, int length, TagConsumer consumer) {
    int start = 0;
    String key = null;
    for (int j = offset; j < offset + length; j++) {
      if (tagBuffer[j] == '\0') {
        if (start < 0) {
          start = j + 1;
        } else if (key == null) {
          key = new String(tagBuffer, start, j - start, StandardCharsets.UTF_8);
          start = j + 1;
        } else {
          consumer.consumeTag(key, new String(tagBuffer, start, j - start, StandardCharsets.UTF_8));
          start = j + 1;
          key = null;
        }
      }
    }
  }

  public interface TagConsumer {
    void consumeTag(String key, String value);
  }

  public static byte[] getTagValue(byte[] tagsList, byte[] tagKey) {
    boolean scanningForKey = true;
    boolean tagKeyMisMatch = false;
    boolean tagKeyFound = false;
    int tagScannerIndex = 0;
    int tagKeyIndex = 0;
    int tagValueStartIndex = 0;
    for (int i = 0; i < tagsList.length; i++) {
      // scan till first delimiter '\0' to differentiate between
      // key and value. stored as [tagkey,\0,tagvalue,\0,tagkey,\0,tagvalue,\0]
      if (tagsList[i] != '\0') {
        if (scanningForKey) {
          if (!tagKeyMisMatch) {
            // bytes for tag keys haven't mismatched yet
            // check if byte from tagList matches byte corresponding
            // byte from tagKey
            if (tagsList[i] != tagKey[tagKeyIndex]) {
              tagKeyMisMatch = true;
            }
          }
          tagScannerIndex += 1;
          if (tagKeyIndex < tagKey.length - 1) {
            tagKeyIndex += 1;
          }
        }

      } else {
        if (scanningForKey) {
          // completed one key scan. check if it matched the
          // tagKey of interest
          if (!tagKeyMisMatch && tagScannerIndex == tagKey.length) {
            // the tagKey was exact match. update boolean to reflect this.
            tagKeyFound = true;
            tagValueStartIndex = i + 1;
          }
        } else { // this is tag Value
          if (tagKeyFound) {
            // reached end of tag value AND tagKey previously matched
            // this is the tag value of interest
            return Arrays.copyOfRange(tagsList, tagValueStartIndex, i);
          }
          tagKeyIndex = tagScannerIndex = 0;
          tagKeyMisMatch = false;
        }
        // if we were scanning for key till now, the next entry will be value
        // and vice-versa. flip the state.
        scanningForKey = !scanningForKey;
      }
    }
    return null;
  }

  public static LocalDateTime getPurgeTime() {
    // random minute in two hours between [2, 120]
    int minBound = 2;
    int maxBound = (int) HOURS.toMinutes(2);
    int randomMinute = new Random().nextInt((maxBound - minBound) + 1) + minBound;

    LocalDateTime now = LocalDateTime.now();
    LocalDateTime snappedToCurrentMinute = now.minusSeconds(now.getSecond());
    return snappedToCurrentMinute.plusMinutes(randomMinute);
  }
}
