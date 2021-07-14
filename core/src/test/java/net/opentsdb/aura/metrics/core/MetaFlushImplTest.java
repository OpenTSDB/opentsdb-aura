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

import net.opentsdb.aura.metrics.core.metaflush.TestUploader;
import net.opentsdb.aura.metrics.metaflush.CompressedMetaWriterFactory;
import net.opentsdb.aura.metrics.metaflush.MetaFlushImpl;
import net.opentsdb.aura.metrics.metaflush.Uploader;
import net.opentsdb.aura.metrics.metaflush.UploaderFactory;
import mockit.Expectations;
import net.opentsdb.aura.metrics.TestUtil;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.hashing.HashFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MetaFlushImplTest extends TimeSeriesShardTest {

    private static ShardConfig config = new ShardConfig();
    private static long now = System.currentTimeMillis();
    private static Random random = new Random(now);
    private static HashFunction hashFunction = new XxHash();

    private ShardConfig shardConfig = new ShardConfig();
    private LocalDateTime purgeDateTime = Util.getPurgeTime();

    ExecutorService executorService = new ExecutorService() {
        @Override
        public void shutdown() {

        }

        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return null;
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return null;
        }

        @Override
        public Future<?> submit(Runnable task) {

            task.run();


            return new Future<Object>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return true;
                }

                @Override
                public Object get() throws InterruptedException, ExecutionException {
                    return null;
                }

                @Override
                public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    return null;
                }
            };
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

        @Override
        public void execute(Runnable command) {

        }
    };

    @Test
    public void flushMeta() {
        final String namespace = "Yamas";
        //Meta stuff
        final RawTimeSeriesEncoder encoder = super.encoder;

        TimeSeriesEncoderFactory timeSeriesEncoderFactory = new TimeSeriesEncoderFactory() {
            @Override
            public RawTimeSeriesEncoder create() {
                return encoder;
            }
        };

        int segmentsInATimeSeries =
                StorageMode.LONG_RUNNING.getSegmentsPerTimeseries(shardConfig.retentionHour, shardConfig.segmentSizeHour);
        int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(shardConfig.segmentSizeHour);
        int secondsInATimeSeries = secondsInASegment * segmentsInATimeSeries;
        TimeSeriesRecordFactory timeSeriesRecordFactory =
                new OffHeapTimeSeriesRecordFactory(
                        segmentsInATimeSeries, secondsInATimeSeries);

        final TestUploader testUploader = new TestUploader(0);

        UploaderFactory uploaderFactory = new UploaderFactory() {
            @Override
            public Uploader create(int shard) {
                return testUploader;
            }
        };

        MetaFlushImpl metaFlush = new MetaFlushImpl(
                timeSeriesRecordFactory,
                new CompressedMetaWriterFactory(namespace),
                uploaderFactory,
                executorService,
                metricRegistry,
                namespace,
                0);


        final List<Runnable> shardLevelTasks = new ArrayList<>();

        new Expectations() {{
            purgeService.scheduleAtFixedRate(
                    withCapture(shardLevelTasks),
                    withAny(0L),
                    withAny(0L),
                    withAny(TimeUnit.SECONDS));
            times = 1;
        }};

        TimeSeriesShardIF shard =
                new TimeSeriesShard(
                        0,
                        shardConfig,
                        new LongRunningStorage.LongRunningStorageContext(shardConfig),
                        encoder,
                        metaDataStore,
                        memoryInfoReader,
                        metricRegistry,
                        purgeDateTime,
                        hashFunction,
                        metaFlush,
                        purgeService);

        final Runnable shardLevelTask = shardLevelTasks.get(0);
        System.out.println("Size: "+ shardLevelTasks);
        Assertions.assertNotNull(shardLevelTask);

        String metric = "request.count";
        Map<String, String> tags1 =
                new HashMap() {
                    {
                        put("host", "host1");
                    }
                };
        Map<String, String> tags2 =
                new HashMap() {
                    {
                        put("host", "host2");
                    }
                };

        int ts = (int) (now / 1000);
        int count = 100;
        int[] times = new int[count];
        double[] values = new double[count];

        for (int i = 0; i < count; i++) {
            times[i] = ts + i;
            values[i] = random.nextDouble();
        }

        LowLevelMetricData.HashedLowLevelMetricData event1 = TestUtil.buildEvent(metric, tags1, times, values);
        LowLevelMetricData.HashedLowLevelMetricData event2 = TestUtil.buildEvent(metric, tags2, times, values);

        shard.addEvent(event1);
        shard.addEvent(event2);
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Came here 3");
        shardLevelTask.run();
        try {
            Thread.sleep(1_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final Map.Entry<String, String> entry1 = tags1.entrySet().iterator().next();
        final Map.Entry<String, String> entry2 = tags2.entrySet().iterator().next();

        compare(testUploader, entry1, entry2, metric);

        compare(testUploader, entry1, entry2, metric);

    }

    private void compare(TestUploader testUploader, Map.Entry<String, String> entry1, Map.Entry<String, String> entry2, String metric) {
        testUploader.readLen();
        testUploader.readRecordHeader();
        testUploader.readHash();

        final String sk1 = testUploader.readTag(StandardCharsets.UTF_8);
        final String[] split = sk1.split("\u0000");

        Assertions.assertTrue(split[0].equals(entry1.getKey()) || split[0].equals(entry2.getKey()));

        if(entry1.getKey().equals(entry2.getKey())) {
            Assertions.assertTrue(split[1].equals(entry1.getValue()) || split[1].equals(entry2.getValue()));
        } else {
            final Map.Entry<String, String> entry = split[0].equals(entry1.getKey()) ? entry1 : entry2;

            Assertions.assertEquals(entry.getValue(), split[1]);
        }

        final String sv1 = testUploader.readMetric(StandardCharsets.UTF_8);

        Assertions.assertEquals(sv1, metric);
    }
}
