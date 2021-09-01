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

package net.opentsdb.aura.metrics.meta;

import io.ultrabrew.metrics.MetricRegistry;
import mockit.Expectations;
import mockit.Mocked;
import net.opentsdb.aura.metrics.core.MemoryInfoReader;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.aura.metrics.core.data.InSufficientBufferLengthException;
import mockit.Injectable;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.hashing.HashFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static net.opentsdb.aura.metrics.TestUtil.buildEvent;
import static net.opentsdb.aura.metrics.core.TimeSeriesShardIF.DOCID_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NewDocStoreTest {

  @Injectable
  private MemoryInfoReader memoryInfoReader;
  @Injectable
  private TimeSeriesShardIF shard;
  @Injectable
  private MetricRegistry registry;
  @Injectable
  private HashFunction hashFunction;

  private String metric = "system.cpu.usr";
  private long[] results = new long[4096];
  private static long now = System.currentTimeMillis();
  private static Random random = new Random(now);

  @Test
  public void testAdd() throws Exception {
    NewDocStore docStore = new NewDocStore(shard);

    String[] tagNames = {"host", "colo"};
    for (int i = 0; i < 100; i++) {
      docStore.add(tagNames, new String[]{"host-" + i, "colo" + (i % 2)}, i);
    }

    Filter filter =
        new LiteralFilter.Builder().forTag("host").withValues("host-1", "host-16", "host-89").build();
    Query query = QueryBuilder.newBuilder().fromNestedFilter(filter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(3, read);
    assertArraysEquals(new long[]{1, 16, 89}, results);
  }

  @Test
  public void testSize() {
    NewDocStore docStore = new NewDocStore(shard);

    assertEquals(0, docStore.size());

    String[] tagNames = {"host", "colo"};
    for (int i = 0; i < 1; i++) {
      docStore.add(tagNames, new String[]{"host-" + i, "colo" + (i % 2)}, i);
    }
    assertEquals(1, docStore.size());

    for (int i = 0; i < 9; i++) {
      docStore.add(tagNames, new String[]{"host-" + i, "colo" + (i % 2)}, i);
    }
    assertEquals(10, docStore.size());

    for (int i = 0; i < 100; i++) {
      docStore.add(tagNames, new String[]{"host-" + i, "colo" + (i % 2)}, i);
    }
    assertEquals(110, docStore.size());
  }

  @Test
  public void testGrowDocTable() {
    NewDocStore docStore = new NewDocStore(DOCID_BATCH_SIZE, shard, hashFunction, 5, 1024, null, false);

    String[] tagNames = {"host", "colo"};
    for (int i = 0; i < 5; i++) {
      docStore.add(tagNames, new String[]{"host-" + i, "colo" + (i % 2)}, i);
    }
    assertEquals(5, docStore.size());

    for (int i = 0; i < 100; i++) {
      docStore.add(tagNames, new String[]{"host-" + i, "colo" + (i % 2)}, i);
    }
    assertEquals(105, docStore.size());
  }

  @Test
  public void testDoesNotGrowBeyondTheMaxCapacity() {
    NewDocStore docStore = new NewDocStore(DOCID_BATCH_SIZE, shard, hashFunction, 5, 9, null, false);
    String[] tagNames = {"host", "colo"};
    for (int i = 0; i < 9; i++) {
      docStore.add(tagNames, new String[]{"host-" + i, "colo" + (i % 2)}, i);
    }

    assertThrows(IllegalStateException.class, () -> {
      docStore.add(tagNames, new String[]{"host-" + 10, "colo" + (10 % 2)}, 10);
    });
  }

  @Test
  public void testInvalidCapacity() {
    assertThrows(IllegalArgumentException.class, () -> {
      new NewDocStore(DOCID_BATCH_SIZE, shard, hashFunction, 5, 4, null, false);
    });
  }

  public Object[][] notLiteralAndFilters() {
    NewDocStore NewDocStore = createNewDocStore();
    return new Object[][]{
        {NewDocStore, "host-[0-8].*", 9, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99},
        {NewDocStore, "host-\\d[0-9].*", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
        {NewDocStore, ".*", new long[0]},
        {NewDocStore, "host-.*", new long[0]}
    };
  }

  @Test
  public void testMatchAllFilter() throws Exception {
    int n = 10;
    NewDocStore docStore = new NewDocStore(shard);
    String[] tagNames = {"host", "colo", "app"};
    for (int i = 0; i < n; i++) {
      docStore.add(
          tagNames, new String[]{"host-" + i, "colo-" + (i % 2), "app-" + (i % 3)}, i + 1);
    }

    Query query = QueryBuilder.matchAllQuery();
    int read = docStore.search(query, metric, results);
    assertEquals(n, read);
    long[] expected = new long[n];
    for (int i = 0; i < n; i++) {
      expected[i] = i + 1;
    }
    assertArraysEquals(expected, results);
  }

  @Test
  public void testNotLiteralAndFilter() throws Exception {
    NewDocStore docStore = new NewDocStore(shard);

    String[] tagNames = {"host", "colo", "app"};
    for (int i = 0; i < 100; i++) {
      docStore.add(tagNames, new String[]{"host-" + i, "colo-" + (i % 2), "app-" + (i % 3)}, i);
    }

    Filter filter =
        new LiteralFilter.Builder().forTag("host").withValues("host-0", "host-1", "host-2").build();
    Query query =
        QueryBuilder.newBuilder()
            .fromNestedFilter(ChainFilter.newBuilder()
                .and(Filter.MATCH_ALL_FILTER)
                .not(filter)
                .build())
            .setTagCount(100)
            .build();

    long[] expected = new long[97];
    for (int i = 3; i < 100; i++) {
      expected[i - 3] = i;
    }

    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  private Stream<Arguments> regexpOrPatterns() {
    long[] allKey = new long[100];
    for (int i = 0; i < 100; i++) {
      allKey[i] = i;
    }

    NewDocStore docStore = createNewDocStore();
    return Stream.of(
        Arguments.of(docStore, "host-1.*", new long[]{1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}),
        Arguments.of(docStore, "host-\\d5.*", new long[]{15, 25, 35, 45, 55, 65, 75, 85, 95}),
        Arguments.of(docStore, ".*", allKey),
        Arguments.of(docStore, "host-.*", allKey)
    );
  }

  @ParameterizedTest
  @MethodSource("regexpOrPatterns")
  public void testRegexpOrFilterDeprecated(NewDocStore docStore, String pattern, long[] expected) throws Exception {
    Filter filter = new RegexpFilter.Builder().forTag("host").withValues(pattern).build();
    Query query = QueryBuilder.newBuilder().fromNestedFilter(filter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  @ParameterizedTest
  @MethodSource("regexpOrPatterns")
  public void testRegexpOrFilter(NewDocStore docStore, String pattern, long[] expected) throws Exception {
    Filter filter = new RegexpFilter.Builder().forTag("host").withValues(pattern).build();
    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(filter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  @ParameterizedTest
  @MethodSource("regexpOrPatterns")
  public void testNestedRegexpFilter(NewDocStore docStore, String pattern, long[] expected) throws Exception {
    RegexpFilter regexpFilter =
        RegexpFilter.newBuilder().forTag("host").withValues(pattern).build();
    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(regexpFilter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  private Stream<Arguments> notRegexpAndPatterns() {
    NewDocStore docStore = createNewDocStore();
    return Stream.of(
        Arguments.of(docStore, "host-[0-8].*", new long[]{9, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99}),
        Arguments.of(docStore, "host-\\d[0-9].*", new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
        Arguments.of(docStore, ".*", new long[0]),
        Arguments.of(docStore, "host-.*", new long[0])
    );
  }

  @ParameterizedTest
  @MethodSource("notRegexpAndPatterns")
  public void testNotRegexpAndFilterDeprecated(
      NewDocStore docStore, String pattern, long[] expected) throws Exception {
    Filter inclusiveFilter = new RegexpFilter.Builder().forTag("host").withValues(".*").build();
    Filter exclusiveFilter = new RegexpFilter.Builder().forTag("host").withValues(pattern).not().build();
    Query query =
        QueryBuilder.newBuilder().fromNestedFilter(
            ChainFilter.newBuilder()
                .and(inclusiveFilter)
                .not(exclusiveFilter)
                .build()).build();
    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  @ParameterizedTest
  @MethodSource("notRegexpAndPatterns")
  public void testNotRegexpAndFilter(NewDocStore docStore, String pattern, long[] expected) throws Exception {
    Filter exclusiveFilter = new RegexpFilter.Builder().forTag("host").withValues(pattern).not().build();
    Query query =
        QueryBuilder.newBuilder()
            .setTagCount(1)
            .fromNestedFilter(
                ChainFilter.newBuilder()
                    .and((Filter) Filter.MATCH_ALL_FILTER)
                    .not(exclusiveFilter)
                    .build()).build();
    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  @ParameterizedTest
  @MethodSource("notRegexpAndPatterns")
  public void testNotRegexpAndFromTSBDFilter(NewDocStore docStore, String pattern, long[] expected) throws Exception {
    Query query = new QueryBuilder()
        .fromNestedFilter(ChainFilter.newBuilder()
            .not(new RegexpFilter.Builder().forTag("host").withValues(pattern).build())
            .build())
        .build();
    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  @ParameterizedTest
  @MethodSource("notRegexpAndPatterns")
  public void testNestedNotRegexpFilter(NewDocStore docStore, String pattern, long[] expected) throws Exception {
    RegexpFilter notRegexpFilter =
        RegexpFilter.newBuilder().forTag("host").withValues(pattern).not().build();
    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(notRegexpFilter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  @Test
  public void testMultipleInclusive() throws Exception {
    NewDocStore docStore = createNewDocStore();
    Filter hostFilter =
        new LiteralFilter.Builder()
            .forTag("host")
            .withValues("host-1", "host-16", "host-89", "host-23", "host-91")
            .build();
    Filter appFilter = new RegexpFilter.Builder().forTag("app").withValues("app-2.*").build();
    Query query =
        QueryBuilder.newBuilder()
            .setTagCount(1)
            .fromNestedFilter(
                ChainFilter.newBuilder()
                    .and(hostFilter)
                    .and(appFilter)
                    .build()).build();
    int read = docStore.search(query, metric, results);
    assertEquals(2, read);
    assertArraysEquals(new long[]{23, 89}, results);
  }

  @Test
  public void testNestedFilterToMatchMultiplevalues() throws Exception {
    NewDocStore docStore = createNewDocStore();
    RegexpFilter appFilter = RegexpFilter.newBuilder().forTag("app").withValues("app-2.*").build();
    LiteralFilter hostFilter =
        LiteralFilter.newBuilder()
            .forTag("host")
            .withValues("host-1", "host-16", "host-89", "host-23", "host-91")
            .build();
    ChainFilter filter =
        ChainFilter.newBuilder(appFilter).and(hostFilter).build();
    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(filter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(2, read);
    assertArraysEquals(new long[]{23, 89}, results);
  }

  @Test
  public void testRegexpOrFilters() throws Exception {
    NewDocStore docStore = createNewDocStore();

    RegexpFilter hostFilter =
        RegexpFilter.newBuilder().forTag("host").withValues("host-1.*").build();
    RegexpFilter appFilter = RegexpFilter.newBuilder().forTag("app").withValues("app-1.*").build();
    RegexpFilter hostFilter2 =
        RegexpFilter.newBuilder().forTag("host").withValues("host-notfound.*").build();

    ChainFilter filter =
        ChainFilter.newBuilder(hostFilter)
            .or(appFilter)
            .or(hostFilter2)
            .build();
    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(filter).build();
    long[] expected = new long[]{
        1, 4, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49,
        52, 55, 58, 61, 64, 67, 70, 73, 76, 79, 82, 85, 88, 91, 94, 97
    };
    int read = docStore.search(query, metric, results);
    assertEquals(expected.length, read);
    assertArraysEquals(expected, results);
  }

  @Test
  public void testRegexpAndFilters() throws Exception {
    NewDocStore docStore = createNewDocStore();
    RegexpFilter hostFilter =
        RegexpFilter.newBuilder().forTag("host").withValues("host-1.*").build();
    RegexpFilter coloFilter = RegexpFilter.newBuilder().forTag("colo").withValues("0.*").build();
    ChainFilter filter =
        ChainFilter.newBuilder(hostFilter).and(coloFilter).build();
    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(filter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(5, read);
    assertArraysEquals(new long[]{10, 12, 14, 16, 18}, results);
  }

  @Test
  public void testMixOperatorsInOneLevel() throws Exception {
    NewDocStore docStore = createNewDocStore();
    RegexpFilter hostFilter =
        RegexpFilter.newBuilder().forTag("host").withValues("host-2.*").build();
    RegexpFilter coloFilter = RegexpFilter.newBuilder().forTag("colo").withValues("1").build();
    LiteralFilter hostLiteralFilter =
        LiteralFilter.newBuilder().forTag("host").withValues("host-23", "host-25").build();
    ChainFilter filter =
        ChainFilter.newBuilder(hostFilter)
            .and(coloFilter)
            .not(hostLiteralFilter)
            .build();

    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(filter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(3, read);
    assertArraysEquals(new long[]{21, 27, 29}, results);
  }

  @Test
  public void testChainedNotFilterAtLevel1() throws Exception {
    NewDocStore docStore = createNewDocStore();

    RegexpFilter hostFilter =
        RegexpFilter.newBuilder().forTag("host").withValues("host-(1|2|3|4|5|6|7|8).*").build();
    LiteralFilter coloFilter =
        LiteralFilter.newBuilder().forTag("colo").withValues("colo-1").withOperator(Filter.Operator.NOT).build();
    ChainFilter chainFilter =
        ChainFilter.newBuilder(hostFilter).or(coloFilter).not().build();

    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(chainFilter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(6, read);
    assertArraysEquals(new long[]{0, 90, 92, 94, 96, 98}, results);
  }

  //  @Test
//  public void testChainedNotFilterAtLevel1BuiltFromTsdbFilter() {
//    /**
//     * filter: NOT (k1=v1 AND k2=v2)
//     *
//     * <p>assert k1=v1, k2=v3 or k1=v4, k2=v2
//     */
//    NewDocStore NewDocStore = new NewDocStore(shard);
//    String[] tagNames = {"app", "host"};
//    NewDocStore.add(tagNames, new String[] {"app-1", "host-1"}, 0); // k1=v1, k2=v1
//    NewDocStore.add(tagNames, new String[] {"app-1", "host-2"}, 1); // k1=v1, k2=v2
//    NewDocStore.add(tagNames, new String[] {"app-1", "host-3"}, 2); // k1=v1, k2=v3
//    NewDocStore.add(tagNames, new String[] {"app-4", "host-2"}, 3); // k1=v4, k2=v2
//
//    TagValueLiteralOrFilter appFilter =
//            TagValueLiteralOrFilter.newBuilder().setKey("app").setFilter("app-1").build(); // k1=v1
//
//    TagValueLiteralOrFilter hostFilter =
//            TagValueLiteralOrFilter.newBuilder().setKey("host").setFilter("host-2").build(); // k2=v2
//
//    net.opentsdb.query.filter.ChainFilter chainFilter =
//            net.opentsdb.query.filter.ChainFilter.newBuilder()
//                    .setOp(net.opentsdb.query.filter.ChainFilter.FilterOp.AND)
//                    .addFilter(appFilter)
//                    .addFilter(hostFilter)
//                    .build(); // (k1=v1 AND k2=v2)
//
//    NotFilter notFilter =
//            NotFilter.newBuilder().setFilter(chainFilter).build(); // NOT (k1=v1 AND k2=v2)
//
//    Query query = newBuilder().fromTSDBQueryFilter(notFilter).setTagCount(2).build();
//    long[] keys = NewDocStore.find(query);
//    assertEquals(keys, new long[] {0, 2, 3}, Arrays.toString(keys));
//  }
//
  @Test
  public void testChainedNotFilterAtLevel2() throws Exception {
    NewDocStore docStore = createNewDocStore();

    RegexpFilter hostFilter =
        RegexpFilter.newBuilder().forTag("host").withValues("host-2.*").build();
    RegexpFilter coloFilter = RegexpFilter.newBuilder().forTag("colo").withValues("0.*").build();
    LiteralFilter hostNotFilter =
        LiteralFilter.newBuilder().forTag("host").withValues("host-22", "host-24").build();
    LiteralFilter appNotFilter =
        LiteralFilter.newBuilder().forTag("app").withValues("app-1").build();

    ChainFilter chainNotFilter =
        ChainFilter.newBuilder(hostNotFilter).and(appNotFilter).not().build();
    ChainFilter filter =
        ChainFilter.newBuilder(hostFilter)
            .and(coloFilter)
            .not(chainNotFilter)
            .build();

    Query query = QueryBuilder.newBuilder().setTagCount(1).fromNestedFilter(filter).build();
    int read = docStore.search(query, metric, results);
    assertEquals(5, read);
    assertArraysEquals(new long[]{2, 20, 24, 26, 28}, results);
  }

//  @DataProvider
//  public Object[][] notFilterAtTheBeginning() {
//    NewDocStore NewDocStore = createNewDocStore();
//
//    TagValueRegexFilter hostRegexpFilter1 =
//            TagValueRegexFilter.newBuilder().setKey("host").setFilter("host-2(2|4).*").build();
//    ChainFilter hostChainFilter1 =
//            ChainFilter.newBuilder()
//                    .setOp(ChainFilter.FilterOp.OR)
//                    .addFilter(hostRegexpFilter1)
//                    .build();
//    NotFilter notFilter1 = NotFilter.newBuilder().setFilter(hostChainFilter1).build();
//
//    TagValueRegexFilter hostRegexpFilter2 =
//            TagValueRegexFilter.newBuilder().setKey("host").setFilter("host-2.*").build();
//    ChainFilter hostChainFilter2 =
//            ChainFilter.newBuilder()
//                    .setOp(ChainFilter.FilterOp.OR)
//                    .addFilter(hostRegexpFilter2)
//                    .build();
//
//    TagValueRegexFilter hostRegexpFilter3 =
//            TagValueRegexFilter.newBuilder().setKey("host").setFilter("host-28.*").build();
//    ChainFilter hostChainFilter3 =
//            ChainFilter.newBuilder()
//                    .setOp(ChainFilter.FilterOp.OR)
//                    .addFilter(hostRegexpFilter3)
//                    .build();
//    NotFilter notFilter2 = NotFilter.newBuilder().setFilter(hostChainFilter3).build();
//
//    TagValueLiteralOrFilter hostLiteralFilter1 =
//            TagValueLiteralOrFilter.newBuilder().setKey("host").setFilter("host-24|host-25").build();
//    ChainFilter hostChainFilter4 =
//            ChainFilter.newBuilder()
//                    .setOp(ChainFilter.FilterOp.OR)
//                    .addFilter(hostLiteralFilter1)
//                    .build();
//    NotFilter notFilter3 = NotFilter.newBuilder().setFilter(hostChainFilter4).build();
//
//    TagValueRegexFilter hostRegexpFilter4 =
//            TagValueRegexFilter.newBuilder()
//                    .setKey("host")
//                    .setFilter("host-(1|3|4|5|6|7|8|9).*")
//                    .build();
//    ChainFilter hostChainFilter5 =
//            ChainFilter.newBuilder()
//                    .setOp(ChainFilter.FilterOp.OR)
//                    .addFilter(hostRegexpFilter4)
//                    .build();
//    NotFilter notFilter4 = NotFilter.newBuilder().setFilter(hostChainFilter5).build();
//
//    TagValueRegexFilter hostRegexpFilter5 =
//            TagValueRegexFilter.newBuilder().setKey("host").setFilter("host-26.*").build();
//
//    ChainFilter hostChainFilter6 =
//            ChainFilter.newBuilder()
//                    .setOp(ChainFilter.FilterOp.OR)
//                    .addFilter(hostRegexpFilter1)
//                    .addFilter(hostRegexpFilter5)
//                    .build();
//
//    TagValueLiteralOrFilter appLiteralFilter =
//            TagValueLiteralOrFilter.newBuilder().setKey("app").setFilter("app-1|app-2").build();
//    ChainFilter appChainFilter =
//            ChainFilter.newBuilder().setOp(ChainFilter.FilterOp.OR).addFilter(appLiteralFilter).build();
//
//    return new Object[][] {
//            {
//                    NewDocStore,
//                    ChainFilter.newBuilder()
//                            .setOp(ChainFilter.FilterOp.AND)
//                            .addFilter(notFilter1)
//                            .addFilter(hostChainFilter2)
//                            .build(),
//                    2,
//                    20,
//                    21,
//                    23,
//                    25,
//                    26,
//                    27,
//                    28,
//                    29
//            } /* 22 and 24 are excluded*/,
//            {
//                    NewDocStore,
//                    ChainFilter.newBuilder()
//                            .setOp(ChainFilter.FilterOp.AND)
//                            .addFilter(notFilter1)
//                            .addFilter(notFilter2)
//                            .addFilter(hostChainFilter2)
//                            .build(),
//                    2,
//                    20,
//                    21,
//                    23,
//                    25,
//                    26,
//                    27,
//                    29
//            },
//            {
//                    NewDocStore,
//                    ChainFilter.newBuilder()
//                            .setOp(ChainFilter.FilterOp.AND)
//                            .addFilter(notFilter1)
//                            .addFilter(notFilter3)
//                            .addFilter(hostChainFilter2)
//                            .build(),
//                    2,
//                    20,
//                    21,
//                    23,
//                    26,
//                    27,
//                    28,
//                    29
//            },
//            {
//                    NewDocStore,
//                    ChainFilter.newBuilder()
//                            .setOp(ChainFilter.FilterOp.AND)
//                            .addFilter(notFilter1)
//                            .addFilter(notFilter3)
//                            .addFilter(hostChainFilter2)
//                            .build(),
//                    2,
//                    20,
//                    21,
//                    23,
//                    26,
//                    27,
//                    28,
//                    29
//            },
//            {
//                    NewDocStore,
//                    ChainFilter.newBuilder().addFilter(notFilter1).addFilter(notFilter4).build(),
//                    0,
//                    2,
//                    20,
//                    21,
//                    23,
//                    25,
//                    26,
//                    27,
//                    28,
//                    29
//            },
//            {
//                    NewDocStore,
//                    ChainFilter.newBuilder()
//                            .addFilter(notFilter1)
//                            .addFilter(hostRegexpFilter2)
//                            .addFilter(hostLiteralFilter1)
//                            .setOp(ChainFilter.FilterOp.OR)
//                            .build(),
//                    2,
//                    20,
//                    21,
//                    23,
//                    25,
//                    26,
//                    27,
//                    28,
//                    29
//            },
//            {
//                    NewDocStore,
//                    ChainFilter.newBuilder()
//                            .addFilter(notFilter4)
//                            .addFilter(hostChainFilter6)
//                            .addFilter(appChainFilter)
//                            .build(),
//                    22,
//                    26,
//            }
//    };
//  }
//
//  @Test(dataProvider = "notFilterAtTheBeginning")
//  public void testNotFilterAtTheBeginningOfTheChain(
//          NewDocStore NewDocStore, QueryFilter tsdbFilter, long... expected) {
//    Query query = newBuilder().fromTSDBQueryFilter(tsdbFilter).setTagCount(1).build();
//    System.out.println(query.getFilter());
//    long[] keys = NewDocStore.find(query);
//    assertEquals(keys, expected, Arrays.toString(keys));
//  }
//
//  @Test
//  public void testRemove() {
//    NewDocStore NewDocStore = createNewDocStore();
//    assertEquals(NewDocStore.size(), 100);
//
//    Filter appFilter = new LiteralFilter.Builder().forTag("app").withValues("app-2").build();
//    Filter coloFilter = new LiteralFilter.Builder().forTag("colo").withValues("colo-0").build();
//    Filter hostFilter = new LiteralFilter.Builder().forTag("host").withValues("host-14").build();
//    Query query =
//            newBuilder()
//                    .exactMatch(true)
//                    .fromNestedFilter(
//                            net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
//                                    .and(appFilter)
//                                    .and(hostFilter)
//                                    .and(coloFilter)
//                                    .build()).build();
//
//    assertEquals(NewDocStore.find(query).length, 1);
//
//    assertTrue(NewDocStore.remove(query));
//    assertFalse(NewDocStore.remove(query));
//    assertEquals(NewDocStore.size(), 99);
//    assertEquals(NewDocStore.find(query).length, 0);
//
//    appFilter = new LiteralFilter.Builder().forTag("app").withValues("app-0").build();
//    coloFilter = new LiteralFilter.Builder().forTag("colo").withValues("colo-1").build();
//    hostFilter = new LiteralFilter.Builder().forTag("host").withValues("host-15").build();
//    query =
//            newBuilder()
//                    .exactMatch(true)
//                    .fromNestedFilter(
//                            net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
//                                    .and(appFilter)
//                                    .and(hostFilter)
//                                    .and(coloFilter)
//                                    .build()).build();
//    assertEquals(NewDocStore.find(query).length, 1);
//  }
//
//  @Test
//  public void testRemoveOrphanTagKeyAndValue() {
//    NewDocStore NewDocStore = new NewDocStore(shard);
//    String colo = "colo";
//    String host = "host";
//    String app = "app";
//
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host1", "app1"}, 1);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host1", "app2"}, 2);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host2", "app2"}, 3);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"bf1", "host1", "app1"}, 4);
//
//    Filter coloFilter = new LiteralFilter.Builder().forTag(colo).withValues("gq1").build();
//    Filter hostFilter = new LiteralFilter.Builder().forTag(host).withValues("host1").build();
//    Filter appFilter = new LiteralFilter.Builder().forTag(app).withValues("app1").build();
//    Query query =
//            newBuilder()
//                    .fromNestedFilter(
//                            net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
//                                    .and(coloFilter)
//                                    .and(hostFilter)
//                                    .and(appFilter).build())
//                    .exactMatch(true)
//                    .build();
//
//    assertTrue(NewDocStore.remove(query));
//    assertEquals(NewDocStore.size(), 4);
//
//    Query allGq1 = newBuilder().fromNestedFilter(coloFilter).build();
//    assertEquals(NewDocStore.find(allGq1).length, 2);
//
//    appFilter = new LiteralFilter.Builder().forTag(app).withValues("app2").build();
//    query =
//            newBuilder()
//                    .fromNestedFilter(
//                            net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
//                                    .and(coloFilter)
//                                    .and(hostFilter)
//                                    .and(appFilter).build())
//                    .exactMatch(true)
//                    .build();
//    assertTrue(NewDocStore.remove(query));
//
//    allGq1 = newBuilder().fromNestedFilter(coloFilter).build();
//    assertEquals(NewDocStore.find(allGq1).length, 1);
//
//    hostFilter = new LiteralFilter.Builder().forTag(host).withValues("host2").build();
//    query =
//            newBuilder()
//                    .fromNestedFilter(
//                            net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
//                                    .and(coloFilter)
//                                    .and(hostFilter)
//                                    .and(appFilter).build())
//                    .exactMatch(true)
//                    .build();
//    assertTrue(NewDocStore.remove(query));
//
//    allGq1 = newBuilder().fromNestedFilter(coloFilter).build();
//    assertEquals(NewDocStore.find(allGq1).length, 0);
//
//    assertEquals(NewDocStore.size(), 1);
//  }
//
//  @Test(expectedExceptions = UnsupportedOperationException.class)
//  public void testOnlyDeletesThroughExactMatchQuery() {
//    NewDocStore NewDocStore = new NewDocStore(shard);
//    String colo = "colo";
//    String host = "host";
//    String app = "app";
//
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host1", "app1"}, 1);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host1", "app2"}, 2);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host2", "app2"}, 3);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"bf1", "host1", "app1"}, 4);
//
//    Filter coloFilter = new LiteralFilter.Builder().forTag(colo).withValues("gq1").build();
//    Filter hostFilter = new LiteralFilter.Builder().forTag(host).withValues("host1").build();
//    Filter appFilter = new LiteralFilter.Builder().forTag(app).withValues("app1").build();
//    Query query =
//            newBuilder().fromNestedFilter(
//                    net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
//                            .and(coloFilter)
//                            .and(hostFilter)
//                            .and(appFilter).build())
//                    .exactMatch(false)
//                    .build();
//
//    NewDocStore.remove(query);
//  }
//
//  @Test
//  public void testRemovesIfQueryMatchesExactlyOneRecord() {
//    NewDocStore NewDocStore = new NewDocStore(shard);
//    String colo = "colo";
//    String host = "host";
//    String app = "app";
//
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host1", "app1"}, 1);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host1", "app2"}, 2);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"gq1", "host2", "app2"}, 3);
//    NewDocStore.add(new String[] {colo, host, app}, new String[] {"bf1", "host1", "app1"}, 4);
//
//    Filter coloFilter = new LiteralFilter.Builder().forTag(colo).withValues("gq1").build();
//    Filter hostFilter = new LiteralFilter.Builder().forTag(host).withValues("host1").build();
//    Query query =
//            newBuilder().fromNestedFilter(
//                    net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
//                            .and(coloFilter)
//                            .and(hostFilter).build())
//                    .exactMatch(true)
//                    .build();
//
//    assertEquals(NewDocStore.size(), 4);
//    NewDocStore.remove(query);
//    assertEquals(NewDocStore.size(), 4);
//  }
//
//  @DataProvider
//  public Object[][] performanceFilters() {
//    return new Object[][] {
//            {
//                    new LiteralFilter.Builder()
//                            .forTag("host")
//                            .withValues("host-1", "host-16", "host-89", "host-369", "host-678")
//                            .build(),
//                    new LiteralFilter.Builder()
//                            .forTag("appId")
//                            .withValues("appId-1", "appId-16", "appId-34", "appId-98", "appId-67")
//                            .build(),
//            },
//            {
//                    new RegexpFilter.Builder().forTag("host").withValues("host-.*").build(),
//                    new LiteralFilter.Builder()
//                            .forTag("appId")
//                            .withValues("appId-1", "appId-16", "appId-34", "appId-98", "appId-67")
//                            .build()
//            },
//    };
//  }
//
//  @Test(dataProvider = "performanceFilters", enabled = false)
//  public void testPerformance(Filter hostFilter, Filter appIdFilter) throws InterruptedException {
//
//    NewDocStore NewDocStore = new NewDocStore(shard, 1_000_000);
//
//    int appLength = 10;
//    int hostLength = 1000;
//    int appIdLength = 100;
//
//    String[] tagNames = {"app", "host", "appId"};
//    Random random = new Random();
//
//    long start = System.currentTimeMillis();
//    for (int i = 0; i < appLength; i++) {
//      String app = "app-" + i;
//      for (int j = 0; j < hostLength; j++) {
//        String host = "host-" + j;
//        for (int k = 0; k < appIdLength; k++) {
//          String appId = "appId-" + k;
//          NewDocStore.add(tagNames, new String[] {app, host, appId}, random.nextLong());
//        }
//      }
//    }
//    long end = System.currentTimeMillis();
//
//    System.out.println("Insertion time: " + (end - start) + " ms");
//
//    Query query =
//            //newBuilder().addInclusiveFilters(hostFilter).addExclusiveFilters(appIdFilter).build();
//            newBuilder().fromNestedFilter(
//                    net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
//                            .addChain(hostFilter, appIdFilter).build()).build();
//
//    start = System.currentTimeMillis();
//    List<Thread> threads = new ArrayList<>();
//
//    Map<String, double[]> times = new HashMap<>();
//
//    for (int i = 0; i < 30; i++) {
//      final int fi = i;
//      Thread thread =
//              new Thread(
//                      () -> {
//                        double[] threadTime = new double[100];
//                        times.put(String.valueOf(fi), threadTime);
//                        for (int j = 0; j < 100; j++) {
//                          long s = System.nanoTime();
//                          NewDocStore.find(query);
//                          long e = System.nanoTime();
//                          System.out.println(
//                                  "Thread- " + fi + " Times: " + (double) (e - s) / 1000 + " us");
//                          threadTime[j] = (double) (e - s) / 1000;
//                        }
//                      });
//      thread.start();
//      threads.add(thread);
//    }
//
//    for (Thread t : threads) {
//      t.join();
//    }
//    end = System.currentTimeMillis();
//    System.out.println("Search time: " + (end - start) + " ms");
//
//    for (Map.Entry<String, double[]> entry : times.entrySet()) {
//      String threadid = entry.getKey();
//      double[] value = entry.getValue();
//      System.out.println("Threadid: " + threadid + " times in us: " + Arrays.toString(value));
//    }
//  }

  @ParameterizedTest(name = "[{index}] add to docstore with {1}")
  @MethodSource("docStoreProvider")
  void addHashedLowLevelMetric(NewDocStore docStore, String displayName) {
    String metric = "request.count";
    Map<String, String> tags1 =
        new HashMap() {
          {
            put("host", "host1");
            put("colo", "colo1");
          }
        };
    Map<String, String> tags2 =
        new HashMap() {
          {
            put("host", "host2");
          }
        };

    int ts = (int) (now / 1000);
    int count = 1;
    int[] times = new int[count];
    double[] values = new double[count];

    for (int i = 0; i < count; i++) {
      times[i] = ts + i;
      values[i] = random.nextDouble();
    }

    LowLevelMetricData.HashedLowLevelMetricData event1 = buildEvent(metric, tags1, times, values);
    LowLevelMetricData.HashedLowLevelMetricData event2 = buildEvent(metric, tags2, times, values);

    docStore.add(event1);
    docStore.add(event2);

    assertEquals(2, docStore.size());
  }

  @ParameterizedTest(name = "[{index}] add Metric to docstore with {1}")
  @MethodSource("docStoreProvider")
  void addMetric(NewDocStore docStore, String displayName) {
    String metric1 = "request.count";
    String metric2 = "error.count";
    Map<String, String> tags1 =
        new HashMap() {
          {
            put("host", "host1");
            put("colo", "colo1");
          }
        };
    Map<String, String> tags2 =
        new HashMap() {
          {
            put("host", "host2");
          }
        };

    int ts = (int) (now / 1000);
    int count = 1;
    int[] times = new int[count];
    double[] values = new double[count];

    for (int i = 0; i < count; i++) {
      times[i] = ts + i;
      values[i] = random.nextDouble();
    }

    LowLevelMetricData.HashedLowLevelMetricData event1 = buildEvent(metric1, tags1, times, values);

    docStore.add(event1);

    assertEquals(1 ,docStore.size());

    LowLevelMetricData.HashedLowLevelMetricData event2 = buildEvent(metric2, tags1, times, values);
    docStore.addMetric(event2);

    assertEquals(1, docStore.size());
  }

  @ParameterizedTest(name = "[{index}] remove from docstore with {1}")
  @MethodSource("docStoreProvider")
  void testRemove(NewDocStore docStore, String displayName) throws InSufficientBufferLengthException {

    String metric = "request.count";
    Map<String, String> tags1 =
        new HashMap() {
          {
            put("host", "host1");
            put("colo", "colo1");
          }
        };
    Map<String, String> tags2 =
        new HashMap() {
          {
            put("host", "host2");
          }
        };

    int ts = (int) (now / 1000);
    int count = 1;
    int[] times = new int[count];
    double[] values = new double[count];

    for (int i = 0; i < count; i++) {
      times[i] = ts + i;
      values[i] = random.nextDouble();
    }

    LowLevelMetricData.HashedLowLevelMetricData event1 = buildEvent(metric, tags1, times, values);
    LowLevelMetricData.HashedLowLevelMetricData event2 = buildEvent(metric, tags2, times, values);

    docStore.add(event1);
    docStore.add(event2);

    assumeTrue(docStore.size() == 2);

    long[] docIdBatch = {event1.tagsSetHash()};
    byte[] byteBuffer = event1.tagsBuffer();

    docStore.remove(docIdBatch, 1, byteBuffer);

    assertEquals(1, docStore.size());
  }

  @Test
  void testSearch() {
    long[] idBatch = new long[]{-10, -100, 1000, 45454, 6454, 1, 2, 0, 0, 0, 0, 0, 0};
    Arrays.sort(idBatch, 0, idBatch.length);
    System.out.println(Arrays.toString(idBatch));
  }

  private Stream<Arguments> docStoreProvider() {
    return Stream.of(
        arguments(NewDocStore.newInstance(shard, false), " Meta query disabled"),
        arguments(NewDocStore.newInstance(shard, true), " Meta query Enabled")
    );
  }

  private NewDocStore createNewDocStore() {
    NewDocStore docStore = new NewDocStore(shard);
    String[] tagNames = {"host", "colo", "app"};
    for (int i = 0; i < 100; i++) {
      String[] strs = new String[]{"host-" + i, "colo-" + (i % 2), "app-" + (i % 3)};
      docStore.add(tagNames, strs, i);
    }
    return docStore;
  }

  void assertArraysEquals(long[] expected, long[] underTest) {
    if (underTest.length < expected.length) {
      throw new AssertionError("Length was " + underTest.length + " but we expected " + expected.length);
    }
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != underTest[i]) {
        throw new AssertionError("Got " + underTest[i] + "@" + i + " and expected " + expected[i]);
      }
    }
  }
}
