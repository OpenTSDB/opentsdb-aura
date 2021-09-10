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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.command.ReadCommand;
import com.aerospike.client.policy.WritePolicy;
import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.downsample.Aggregator;
import net.opentsdb.aura.metrics.core.downsample.AggregatorIterator;
import net.opentsdb.aura.metrics.core.downsample.AverageAggregator;
import net.opentsdb.aura.metrics.core.downsample.CountAggregator;
import net.opentsdb.aura.metrics.core.downsample.DownSampler;
import net.opentsdb.aura.metrics.core.downsample.Interval;
import net.opentsdb.aura.metrics.core.downsample.MaxAggregator;
import net.opentsdb.aura.metrics.core.downsample.MinAggregator;
import net.opentsdb.aura.metrics.core.downsample.SegmentWidth;
import net.opentsdb.aura.metrics.core.downsample.SumAggregator;
import net.opentsdb.aura.metrics.core.downsample.SumOfSquareAggregator;
import net.opentsdb.aura.metrics.core.gorilla.GorillaDownSampledTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.OffHeapGorillaDownSampledSegment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static net.opentsdb.aura.metrics.LTSAerospike.KEY_LENGTH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class AerospikeFunctionalTest {

  private static final int SEGMENT_TIMESTAMP = 1611288000;

  AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
  WritePolicy writePolicy = new WritePolicy();

  @Test
  void readSet() {
    Key key = new Key("default", "test", 123);
    Record record = client.get(null, key);
    System.out.println(record.toString());
  }

  @Test
  void writeMap() {
    Map<Integer, String> userMap = new HashMap<>();
    userMap.put(1, "u1");
    userMap.put(2, "u2");
    userMap.put(3, "u3");
    userMap.put(4, "u4");
    Key key = new Key("default", "test", "userMap");
    Bin bin = new Bin("map", userMap);
    client.put(writePolicy, key, bin);
  }

  @Test
  void readMap() {
    Key key = new Key("default", "test", "userMap");
    Record record = client.get(null, key);
    System.out.println(record.toString());
  }

  @Test
  void readMapEntries() {
    Key recordKey = new Key("default", "test", "userMap");
    List<Value> keyList = new ArrayList<>();
    keyList.add(Value.get(1));
    keyList.add(Value.get(4));
    Operation operation = MapOperation.getByKeyList("", keyList, MapReturnType.VALUE);
    //    Operation operation = MapOperation.size("");
    Record record = client.operate(null, recordKey, operation);
    System.out.println(record.toString());
    System.out.println(record.getList("").toString());
  }

  @Test
  void writeByteArraysToMap() {
    Map<Integer, byte[]> map = new HashMap<>();
    byte[] v1 = new byte[] {1};
    byte[] v2 = new byte[] {2, 2};
    byte[] v3 = new byte[] {3, 3, 3};
    byte[] v4 = new byte[] {4, 4, 4, 4};
    byte[] v5 = new byte[] {5, 5, 5, 5, 5};
    map.put(1, v1);
    map.put(2, v2);
    map.put(3, v3);
    map.put(4, v4);
    map.put(5, v5);

    Key key = new Key("default", "rollup", 0);
    Bin bin = new Bin(null, map);
    client.put(writePolicy, key, bin);
  }

  @Test
  void deleteMapEntries() {
    Key recordKey = new Key("default", "rollup", 0);
    Operation operation = MapOperation.removeByIndexRange(null, 0, MapReturnType.KEY);
    Record record = client.operate(null, recordKey, operation);
    System.out.println(record.toString());
  }

  @Test
  void readMapEntriesAsRawBytes() {
    Key recordKey = new Key("default", "rollup", 0);
    List<Value> keyList = Arrays.asList(Value.get(1), Value.get(3), Value.get(5));

    Operation operation = MapOperation.getByKeyList("", keyList, MapReturnType.VALUE);
    //    Operation operation = MapOperation.getByKey("", Value.get(5), MapReturnType.VALUE);
    //    Operation operation = MapOperation.size("");

    Record record = client.operate(null, recordKey, operation);

    WritePolicy readDefault = new WritePolicy(client.readPolicyDefault);
    OperateArgs args =
        new OperateArgs(
            client.getCluster(),
            null,
            client.getWritePolicyDefault(),
            readDefault,
            recordKey,
            new Operation[] {operation});
    ReadCommand command =
        new ReadCommand(client.getCluster(), args.writePolicy, recordKey, args.partition, true) {

          @Override
          protected void parseResult(Connection conn) throws IOException {
            System.out.println(dataBuffer.length);
            System.out.println(dataOffset);
          }
        };

    command.execute();

    //    System.out.println(record.toString());
    //    System.out.println(record.getList("").get(0));
  }

  @Test
  void serializeDSSegments() {

    Interval interval = Interval._30_SEC;
    SegmentWidth segmentWidth = SegmentWidth._2_HR;
    int intervalWidth = interval.getWidth();
    short intervalCount = interval.getCount(segmentWidth);
    Random random = new Random(System.currentTimeMillis());

    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).avg().sum().count().min().max().sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] randomValues = new double[segmentWidth.getWidth()];
    Arrays.fill(randomValues, Double.NaN);
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = random.nextLong() + random.nextDouble();
    }

    OffHeapGorillaDownSampledSegment segment = new OffHeapGorillaDownSampledSegment(256, null);

    GorillaDownSampledTimeSeriesEncoder encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(randomValues);

    assertEquals(intervalCount, encoder.getNumDataPoints());

    int secondsInRecord = 6 * 3600;
    int secondsInSegment = (int) TimeUnit.HOURS.toSeconds(2);
    int recordTimestamp = encoder.getSegmentTime() - (encoder.getSegmentTime() % secondsInRecord);

    //    long hash = random.nextLong();
    long hash = 8737594818697886573l;
    int recordOffset = (encoder.getSegmentTime() - recordTimestamp) / secondsInSegment;

    System.out.println("hash: " + hash);
    System.out.println("recordTimeStamp: " + recordTimestamp);

    Key key = new Key("default", null, new ByteValue(new byte[KEY_LENGTH]));
    ByteValue v = (ByteValue) key.userKey;
    ByteArrays.putLong(hash, v.bytes, 0);
    ByteArrays.putInt(recordTimestamp, v.bytes, 8);

    Bin bin = new Bin(null, new DSEncodedValue());
    ((DSEncodedValue) bin.value).reset(encoder, recordOffset);

    client.put(writePolicy, key, bin);

    downSampler.apply(randomValues);
    AggregatorIterator<double[]> aggItr = downSampler.iterator();
    double[] expectedAvgValues = aggItr.next();
    double[] expectedSumValues = aggItr.next();
    double[] expectedCountValues = aggItr.next();
    double[] expectedMinValues = aggItr.next();
    double[] expectedMaxValues = aggItr.next();
    double[] expectedSumOfSquaresValues = aggItr.next();

    List<Value> keyList = new ArrayList<>();
    keyList.add(Value.get((byte) (recordOffset << 4 | 0))); // header
    keyList.add(Value.get((byte) (recordOffset << 4 | SumAggregator.ORDINAL)));
    keyList.add(Value.get((byte) (recordOffset << 4 | CountAggregator.ORDINAL)));
    keyList.add(Value.get((byte) (recordOffset << 4 | AverageAggregator.ORDINAL)));
    keyList.add(Value.get((byte) (recordOffset << 4 | MinAggregator.ORDINAL)));
    keyList.add(Value.get((byte) (recordOffset << 4 | MaxAggregator.ORDINAL)));
    keyList.add(Value.get((byte) (recordOffset << 4 | SumOfSquareAggregator.ORDINAL)));

    Operation operation = MapOperation.getByKeyList("", keyList, MapReturnType.VALUE);
    Record record = client.operate(null, key, operation);
    List<byte[]> list = (List<byte[]>) record.getList("");

    assertEquals(7, list.size());

    byte aggId =
        SumAggregator.ID
            | CountAggregator.ID
            | AverageAggregator.ID
            | MinAggregator.ID
            | MaxAggregator.ID
            | SumOfSquareAggregator.ID;

    byte[] aggIds =
        new byte[] {
          0,
          AverageAggregator.ID,
          SumAggregator.ID,
          CountAggregator.ID,
          MinAggregator.ID,
          MaxAggregator.ID,
          SumOfSquareAggregator.ID
        };

    OnHeapAerospikeSegment aerospikeSegment =
        new OnHeapAerospikeSegment(SEGMENT_TIMESTAMP, aggId, aggIds, list);
    AerospikeDSTimeSeriesEncoder reader = new AerospikeDSTimeSeriesEncoder(false, aerospikeSegment);
    double[] values = new double[reader.getIntervalCount()];

    reader.readAggValues(values, SumAggregator.ID);
    assertArrayEquals(expectedSumValues, values);

    reader.readAggValues(values, CountAggregator.ID);
    assertArrayEquals(expectedCountValues, values);

    reader.readAggValues(values, AverageAggregator.ID);
    assertArrayEquals(expectedAvgValues, values);

    reader.readAggValues(values, MinAggregator.ID);
    assertArrayEquals(expectedMinValues, values);

    reader.readAggValues(values, MaxAggregator.ID);
    assertArrayEquals(expectedMaxValues, values);

    reader.readAggValues(values, SumOfSquareAggregator.ID);
    assertArrayEquals(expectedSumOfSquaresValues, values);

    reader.readAggValuesByIndex(values, 2);
    assertArrayEquals(expectedSumValues, values);

    reader.readAggValuesByIndex(values, 3);
    assertArrayEquals(expectedCountValues, values);

    reader.readAggValuesByIndex(values, 1);
    assertArrayEquals(expectedAvgValues, values);

    reader.readAggValuesByIndex(values, 4);
    assertArrayEquals(expectedMinValues, values);

    reader.readAggValuesByIndex(values, 5);
    assertArrayEquals(expectedMaxValues, values);

    reader.readAggValuesByIndex(values, 6);
    assertArrayEquals(expectedSumOfSquaresValues, values);
  }
}
