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

package net.opentsdb;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.oath.auth.KeyRefresher;
import com.oath.auth.KeyRefresherException;
import com.oath.auth.Utils;
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.aws.S3UploaderFactory;
import net.opentsdb.aura.aws.auth.AthensCredentialsProviderImpl;
import net.opentsdb.aura.metrics.ASCluster;
import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.core.EphemeralStorage;
import net.opentsdb.aura.metrics.core.FlushStatus;
import net.opentsdb.aura.metrics.core.Flusher;
import net.opentsdb.aura.metrics.core.LongRunningStorage;
import net.opentsdb.aura.metrics.core.LongTermStorage;
import net.opentsdb.aura.metrics.core.MemoryInfoReader;
import net.opentsdb.aura.metrics.core.OffHeapTimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.ShardConfig;
import net.opentsdb.aura.metrics.core.TSFlusher;
import net.opentsdb.aura.metrics.core.TSFlusherImp;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.core.XxHash;
import net.opentsdb.aura.metrics.core.data.HashTable;
import net.opentsdb.aura.metrics.core.downsample.SegmentWidth;
import net.opentsdb.aura.metrics.core.gorilla.GorillaSegmentFactory;
import net.opentsdb.aura.metrics.core.gorilla.GorillaTimeSeriesDownSamplingEncoderFactory;
import net.opentsdb.aura.metrics.core.gorilla.GorillaTimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.gorilla.OffHeapDownSampledGorillaSegmentFactory;
import net.opentsdb.aura.metrics.core.gorilla.OffHeapGorillaSegmentFactory;
import net.opentsdb.aura.metrics.meta.NewDocStoreFactory;
import net.opentsdb.aura.metrics.metaflush.CompressedMetaWriterFactory;
import net.opentsdb.aura.metrics.metaflush.MetaFlushImpl;
import net.opentsdb.aura.metrics.storage.AerospikeClientPlugin;
import net.opentsdb.aura.metrics.storage.AuraMetricsNumericArrayIterator;
import net.opentsdb.aura.metrics.storage.AuraMetricsSourceFactory;
import net.opentsdb.aura.metrics.system.Rhel7MemoryInfoReader;
import net.opentsdb.collections.LongLongHashTable;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.service.TSDBService;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static net.opentsdb.ConfigUtils.configId;

/**
 * TODO - This is really a stub service to get the in-memory store up and running
 * with some particular plugins (Aerospike and AWS). We need to clean this up and
 * pass the StatsCollector around.
 */
public class AuraMetricsService extends BaseTSDBPlugin implements TSDBService {
  private static final Logger logger = LoggerFactory.getLogger(AuraMetricsService.class);

  public final String TYPE = "AuraMetricsService";

  public static final String SHARD_CONFIG_KEY = "metrics.shard.ids.config.path";
  public static final String SHARD_TIME_KEY = "metrics.shard.times.config.path";
  public static final String SHARD_IDS_KEY = "metrics.shard.ids";
  public static final String SHARD_SEGMENT_HOURS_KEY = "metrics.shard.segment.hours";

  public static final String AWS_ATHENZ_TRUST_STORE_KEY = "metrics.flusher.aws.athenz.truststore.path";
  public static final String AWS_ATHENZ_CERT_KEY = "metrics.flusher.aws.athenz.cert.path";
  public static final String AWS_ATHENZ_KEY_KEY = "metrics.flusher.aws.athenz.key.path";

  public static final String AWS_ATHENZ_ZTS_KEY = "metrics.flusher.aws.athenz.zts.url";
  public static final String AWS_ATHENZ_DOMAIN_KEY = "metrics.flusher.aws.athenz.domain";
  public static final String AWS_ATHENZ_ROLE_KEY = "metrics.flusher.aws.athenz.role";
  public static final String AWS_ATHENZ_EXPIRATION_KEY = "metrics.flusher.aws.athenz.expiration";

  public static final String AWS_S3_BUCKET_KEY = "metrics.flusher.aws.s3.bucket";
  public static final String AWS_S3_REGION_KEY = "metrics.flusher.aws.s3.region";

  public static final String AS_HOSTS_KEY = "metrics.flusher.aerospike.hosts";
  public static final String AS_NAMESPACE_KEY = "metrics.flusher.aerospike.namespace";
  public static final String FLUSH_THREADS_KEY = "metrics.flusher.threads";

  public enum StorageMode {
    EPHEMERAL {
      @Override
      public int getSegmentsPerTimeseries(final int retentionHour, final int segmentSizeHour) {
        return 1;
      }
    },
    LONG_RUNNING {
      @Override
      public int getSegmentsPerTimeseries(final int retentionHour, final int segmentSizeHour) {
        return retentionHour / segmentSizeHour + 1; // one extra window to address the overlap
      }
    };

    public abstract int getSegmentsPerTimeseries(int retentionHour, int segmentSizeHour);
  }

  public enum FlushType {
    META_ONLY,
    TS_ONLY,
    NONE,
    FULL;

    boolean flushMeta() {
      return notNone() && (this != TS_ONLY);
    }

    boolean flushTS() {
      return notNone() && this != META_ONLY;
    }

    boolean notNone() {
      return (this != NONE);
    }
  }

  // TODO plugins
  public enum InputMode {
    KAFKA,   // Use Apache Kafka to poll for data
    PULSAR,  // Use Apache Pulsar to poll data
    DATA_GEN // Use Internal Data Generator to generate and push data
  }

  public enum OperatingMode {
    STANDALONE, // runs it all
    API, // just the API
    WRITER, // just kafka and the API
    STORE // just the store and API
  }

  private List<String> namespaces;
  private ScheduledExecutorService metricsScheduler;
  private StorageMode storageMode;
  private MemoryInfoReader memoryInfoReader;
  private ShardConfig shardConfig;
  private HashFunction hashFunction;
  private TimeSeriesEncoderFactory encoderFactory;
  private TimeSeriesRecordFactory timeSeriesRecordFactory;

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = Strings.isNullOrEmpty(id) ? null : id;

    registerConfigs();

    // TODO - need a plugin for this.
    memoryInfoReader = new Rhel7MemoryInfoReader(1000);
    namespaces = tsdb.getConfig().getTyped(
            configId(id, ConfigUtils.NAMESPACES_KEY), ConfigUtils.STRING_LIST);
    try {
      shardConfig = makeShardConfig();
    } catch (Exception e) {
      logger.error("Failed to initialize Aura Metrics service.", e);
      return Deferred.fromError(e);
    }

    storageMode = StorageMode.valueOf(
      tsdb.getConfig().getString(configId(id, ConfigUtils.STORAGE_MODE_KEY)));

    metricsScheduler = Executors.newSingleThreadScheduledExecutor();
    hashFunction = new XxHash();

    TimeSeriesStorageIf timeSeriesStorage = setupStorage();
    if (timeSeriesStorage == null) {
      return Deferred.fromError(new RuntimeException("Failed to instantiate."));
    }

    AuraMetricsSourceFactory auraSourceFactory =
            (AuraMetricsSourceFactory)
                    tsdb.getRegistry().getPlugin(TimeSeriesDataSourceFactory.class, null);
    auraSourceFactory.setTimeSeriesStorage(timeSeriesStorage);
    auraSourceFactory.setTimeSeriesRecordFactory(timeSeriesRecordFactory);
    auraSourceFactory.setQueryIncludesNamespace(
            tsdb.getConfig().getBoolean(configId(id, ConfigUtils.NAMESPACE_QUERY_KEY)));

    AuraMetricsNumericArrayIterator.timeSeriesEncoderFactory = encoderFactory;
    AuraMetricsNumericArrayIterator.timeSeriesRecordFactory = timeSeriesRecordFactory;

    OperatingMode operatingMode = OperatingMode.valueOf(
            tsdb.getConfig().getString(configId(id, ConfigUtils.OPERATING_MODE_KEY)));
    logger.info("Operation mode: {}", operatingMode);
    tsdb.getRegistry().registerSharedObject("AuraTimeSeriesStorage", timeSeriesStorage);

    logger.info("Successfully initialized Aura Metrics");
    logger.info("Stats collector: " + tsdb.getStatsCollector());
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    if (metricsScheduler != null) {
      metricsScheduler.shutdown();
    }
    return Deferred.fromResult(null);
  }

  private void registerConfigs() {
    ConfigUtils.registerConfigs(tsdb, id);

    if (!tsdb.getConfig().hasProperty(configId(id, SHARD_CONFIG_KEY))) {
      tsdb.getConfig().register(configId(id, SHARD_CONFIG_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, SHARD_TIME_KEY))) {
      tsdb.getConfig().register(configId(id, SHARD_TIME_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, SHARD_IDS_KEY))) {
      tsdb.getConfig().register(configId(id, SHARD_IDS_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, SHARD_SEGMENT_HOURS_KEY))) {
      tsdb.getConfig().register(configId(id, SHARD_SEGMENT_HOURS_KEY), null, false,
              "TODO");
    }

    // TEMP AWS
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_ATHENZ_TRUST_STORE_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_ATHENZ_TRUST_STORE_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_ATHENZ_CERT_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_ATHENZ_CERT_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_ATHENZ_KEY_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_ATHENZ_KEY_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_ATHENZ_ZTS_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_ATHENZ_ZTS_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_ATHENZ_DOMAIN_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_ATHENZ_DOMAIN_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_ATHENZ_ROLE_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_ATHENZ_ROLE_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_ATHENZ_EXPIRATION_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_ATHENZ_EXPIRATION_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_ATHENZ_EXPIRATION_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_ATHENZ_EXPIRATION_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AWS_S3_BUCKET_KEY))) {
      tsdb.getConfig().register(configId(id, AWS_S3_BUCKET_KEY), null, false,
              "TODO");
    }

    // TEMP AS
    if (!tsdb.getConfig().hasProperty(configId(id, AS_HOSTS_KEY))) {
      tsdb.getConfig().register(configId(id, AS_HOSTS_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, AS_NAMESPACE_KEY))) {
      tsdb.getConfig().register(configId(id, AS_NAMESPACE_KEY), null, false,
              "TODO");
    }
    if (!tsdb.getConfig().hasProperty(configId(id, FLUSH_THREADS_KEY))) {
      tsdb.getConfig().register(configId(id, FLUSH_THREADS_KEY), null, false,
              "TODO");
    }
  }

  private TimeSeriesStorageIf setupStorage() {
    final StorageMode storageMode = StorageMode.valueOf(
            tsdb.getConfig().getString(configId(id, ConfigUtils.STORAGE_MODE_KEY)));

    TimeSeriesEncoderFactory dsEncoderFactory = null;
    if (shardConfig.downSampleInterval != null) {
      OffHeapDownSampledGorillaSegmentFactory downSampledSegmentFactory =
              new OffHeapDownSampledGorillaSegmentFactory(shardConfig.segmentBlockSizeBytes, tsdb.getStatsCollector());
      dsEncoderFactory =
              new GorillaTimeSeriesDownSamplingEncoderFactory(
                      shardConfig.lossy,
                      shardConfig.downSampleInterval,
                      shardConfig.segmentWidth,
                      shardConfig.downSampleAggTypes,
                      downSampledSegmentFactory);
    }

    GorillaSegmentFactory segmentFactory =
            new OffHeapGorillaSegmentFactory(shardConfig.segmentBlockSizeBytes, tsdb.getStatsCollector());
    encoderFactory = new GorillaTimeSeriesEncoderFactory(
                    shardConfig.lossy,
                    shardConfig.garbageQSize,
                    shardConfig.segmentCollectionDelayMinutes,
                    tsdb.getStatsCollector(),
                    segmentFactory);
    int segmentsInATimeSeries =
            storageMode.getSegmentsPerTimeseries(shardConfig.retentionHour,
                    shardConfig.segmentSizeHour);
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(shardConfig.segmentSizeHour);
    timeSeriesRecordFactory = new OffHeapTimeSeriesRecordFactory(segmentsInATimeSeries, secondsInASegment);

    final FlushType flushType = FlushType.valueOf(
            tsdb.getConfig().getString(configId(id, ConfigUtils.FLUSH_TYPE_KEY)));
    String duration = tsdb.getConfig().getString(
            configId(id, ConfigUtils.FLUSH_FREQUENCY_KEY));
    final long flushFrequency = DateTime.parseDuration(duration) / 1000;
    final TSFlusher tsFlusher =
            flushType.flushTS()
                    ? createTSFlusher(
                    dsEncoderFactory,
                    secondsInASegment)
                    : null;
    final Flusher metaFlusher;
    try {
      metaFlusher = flushType.flushMeta()
              ? createMetaFlusher()
              : null;
    } catch (InterruptedException e) {
      logger.error("Failed to instantiate meta flusher", e);
      return null;
    } catch (IOException e) {
      logger.error("Failed to instantiate meta flusher", e);
      return null;
    } catch (KeyRefresherException e) {
      logger.error("Failed to instantiate meta flusher", e);
      return null;
    }

    logger.info(
            "Flush type: {} ts flusher: {} meta flusher: {} flush frequency: {}",
            flushType.name(),
            tsFlusher,
            metaFlusher,
            flushFrequency);
    final Flusher flusher =
            (flushType.flushMeta() || flushType.flushTS())
                    ? new Flusher() {
              @Override
              public long frequency() {
                return flushFrequency;
              }

              @Override
              public FlushStatus flushShard(
                      int shardId,
                      HashTable tagTable,
                      HashTable metricTable,
                      LongLongHashTable timeSeriesTable,
                      int flushTimestamp) {
                final AggregateFlushStatus aggStatus = new AggregateFlushStatus();
                if (tsFlusher != null) {
                  aggStatus.trackStatus(
                          tsFlusher.flushShard(shardId, timeSeriesTable, flushTimestamp));
                }
                if (metaFlusher != null) {
                  aggStatus.trackStatus(
                          metaFlusher.flushShard(
                                  shardId, tagTable, metricTable, timeSeriesTable, flushTimestamp));
                }
                return aggStatus;
              }
            }
                    : null;
    final ScheduledExecutorService purgeService = Executors.newSingleThreadScheduledExecutor();
    if (storageMode == StorageMode.LONG_RUNNING) {
      return new LongRunningStorage(
                      memoryInfoReader,
                      shardConfig,
                      tsdb.getStatsCollector(),
                      hashFunction,
                      encoderFactory,
                      new NewDocStoreFactory(shardConfig, hashFunction),
                      flusher,
                      purgeService);
    }

    if (storageMode == StorageMode.EPHEMERAL) {
      return new EphemeralStorage(
                      memoryInfoReader,
                      shardConfig,
                      tsdb.getStatsCollector(),
                      hashFunction,
                      encoderFactory,
                      new NewDocStoreFactory(shardConfig, hashFunction),
                      flusher,
                      purgeService);
    }

    throw new IllegalStateException("Invalid storage mode.");
  }

  private ShardConfig makeShardConfig() throws IOException {
    ShardConfig shardConfig = new ShardConfig();
    String shardIdsString;
    String segmentStartTime;
    if (!Strings.isNullOrEmpty(
            tsdb.getConfig().getString(configId(id, SHARD_CONFIG_KEY)))) {
      String filePath = tsdb.getConfig().getString(configId(id, SHARD_CONFIG_KEY));
      File path = new File(filePath);
      if (!path.exists()) {
        throw new RuntimeException("Shard config file: " + filePath + " not found");
      }
      shardIdsString = com.google.common.io.Files.toString(path, Const.UTF8_CHARSET);
      if (shardIdsString == null || shardIdsString.isEmpty()) {
        throw new RuntimeException("Shard config file: " + filePath + " empty");
      }

      String segmentTimeFilePath = tsdb.getConfig().getString(
              configId(id, SHARD_TIME_KEY));
      path = path = new File(segmentTimeFilePath);
      if (!path.exists()) {
        throw new RuntimeException("Segment time file: " + segmentTimeFilePath + " not found");
      }
      segmentStartTime = com.google.common.io.Files.toString(path, Const.UTF8_CHARSET);
    } else {
      shardIdsString = tsdb.getConfig().getString(
              configId(id, SHARD_IDS_KEY));
      segmentStartTime = tsdb.getConfig().getString(
              configId(id, SHARD_SEGMENT_HOURS_KEY));
    }

    if (null != shardIdsString && !shardIdsString.isEmpty()) {
      int[] shardIds =
              Arrays.stream(shardIdsString.split(","))
                      .map(String::trim)
                      .mapToInt(Integer::parseInt)
                      .toArray();
      shardConfig.shardIds = shardIds;
      shardConfig.shardCount = shardIds.length;
    } else {
      shardConfig.shardCount = tsdb.getConfig().getInt(
              configId(id, ConfigUtils.SHARDS_KEY));
      shardConfig.shardIds = IntStream.range(0, shardConfig.shardCount).toArray();
    }

    if (null != shardIdsString && !shardIdsString.isEmpty()) {
      int[] shardIds =
              Arrays.stream(shardIdsString.split(","))
                      .map(String::trim)
                      .mapToInt(Integer::parseInt)
                      .toArray();
      shardConfig.shardIds = shardIds;
      shardConfig.shardCount = shardIds.length;
    } else {
      shardConfig.shardCount = tsdb.getConfig().getInt(
              configId(id, ConfigUtils.SHARDS_KEY));
      shardConfig.shardIds = IntStream.range(0, shardConfig.shardCount).toArray();
    }

    if (null != segmentStartTime && !segmentStartTime.isEmpty()) {
      int[] segmentStartTimes =
              Arrays.stream(segmentStartTime.split(","))
                      .map(String::trim)
                      .mapToInt(Integer::parseInt)
                      .toArray();
      shardConfig.segmentStartTimes = segmentStartTimes;
    }

    if (namespaces == null || namespaces.isEmpty()) {
      throw new IllegalArgumentException(configId(id, ConfigUtils.NAMESPACES_KEY) +
              " must be a list of one or more namespaces.");
    }

    String duration = tsdb.getConfig().getString(configId(id,
            ConfigUtils.SEGMENT_SIZE_KEY));
    shardConfig.namespace = namespaces.get(0);
    shardConfig.segmentSizeHour = (int) (DateTime.parseDuration(duration) / 1000 / 3600);
    shardConfig.segmentWidth = SegmentWidth.getByHours(shardConfig.segmentSizeHour);
//    shardConfig.downSampleInterval = getDownSampleInterval();
//    shardConfig.downSampleAggTypes = getDownSampleAggs();
    duration = tsdb.getConfig().getString(configId(id,
            ConfigUtils.RETENTION_KEY));
    shardConfig.retentionHour = (int) (DateTime.parseDuration(duration) / 1000 / 3600);
    shardConfig.queueSize = tsdb.getConfig().getInt(
            configId(id, ConfigUtils.SHARD_QUEUE_SIZE_KEY));
    shardConfig.memoryUsageLimitPct = tsdb.getConfig().getInt(
            configId(id, ConfigUtils.MEMORY_USAGE_KEY));
    shardConfig.garbageQSize = tsdb.getConfig().getInt(
            configId(id, ConfigUtils.GARBAGE_QUEUE_SIZE_KEY));

    duration = tsdb.getConfig().getString(configId(id,
            ConfigUtils.GARBAGE_DELAY_KEY));
    shardConfig.segmentCollectionDelayMinutes =
            (int) (DateTime.parseDuration(duration) / 1000 / 60);
    duration = tsdb.getConfig().getString(configId(id,
            ConfigUtils.GARBAGE_FREQUENCY_KEY));
    shardConfig.segmentCollectionFrequencySeconds =
            (int) (DateTime.parseDuration(duration) / 1000);
    shardConfig.metricTableSize = tsdb.getConfig().getInt(
            configId(id, ConfigUtils.SHARD_METRIC_TABLE_KEY));
    shardConfig.tagTableSize = tsdb.getConfig().getInt(
            configId(id, ConfigUtils.SHARD_TAG_TABLE_KEY));
    duration = tsdb.getConfig().getString(configId(id,
            ConfigUtils.PURGE_FREQUENCY_KEY));
    shardConfig.purgeFrequencyMinutes =
            (int) (DateTime.parseDuration(duration) / 1000 / 60);
    shardConfig.metaPurgeBatchSize = tsdb.getConfig().getInt(
            configId(id, ConfigUtils.PURGE_BATCH_SIZE_KEY));
    shardConfig.metaQueryEnabled = true;

    logger.info("Segment start time is {} with retention {} hours.",
            Arrays.toString(shardConfig.segmentStartTimes),
            shardConfig.retentionHour);
    logger.info("Shard ids: {}", Arrays.toString(shardConfig.shardIds));

    return shardConfig;
  }

  private static class AggregateFlushStatus implements FlushStatus {

    private final List<FlushStatus> flushStatuses = new ArrayList<>();

    public void trackStatus(FlushStatus flushStatus) {
      this.flushStatuses.add(flushStatus);
    }

    @Override
    public boolean inProgress() {

      for (FlushStatus flushStatus : flushStatuses) {
        if (flushStatus.inProgress()) {
          return true;
        }
      }
      return false;
    }
  }

  private Flusher createMetaFlusher()
          throws InterruptedException, KeyRefresherException, IOException {

    // Build SSL Context
    logger.info("Creating Meta flusher");
    KeyRefresher keyRefresher =
            Utils.generateKeyRefresher(
                    tsdb.getConfig().getString(configId(id, AWS_ATHENZ_TRUST_STORE_KEY)),
                    "changeit",
                    tsdb.getConfig().getString(configId(id, AWS_ATHENZ_CERT_KEY)),
                    tsdb.getConfig().getString(configId(id, AWS_ATHENZ_KEY_KEY)));

    keyRefresher.startup();

    SSLContext sslContext =
            Utils.buildSSLContext(
                    keyRefresher.getKeyManagerProxy(), keyRefresher.getTrustManagerProxy());

    final AthensCredentialsProviderImpl provider =
            AthensCredentialsProviderImpl.Builder.create()
                    .ztsUrl(tsdb.getConfig().getString(configId(id, AWS_ATHENZ_ZTS_KEY)))
                    .domainName(tsdb.getConfig().getString(configId(id, AWS_ATHENZ_DOMAIN_KEY)))
                    .roleName(tsdb.getConfig().getString(configId(id, AWS_ATHENZ_ROLE_KEY)))
                    .expiryTimeInSecs(tsdb.getConfig().getInt(configId(id, AWS_ATHENZ_EXPIRATION_KEY)))
                    .sslContext(sslContext)
                    .build();

    final S3UploaderFactory s3UploaderFactory =
            S3UploaderFactory.Builder.create()
                    .namespace(namespaces.get(0))
                    .bucketName(tsdb.getConfig().getString(configId(id, AWS_S3_BUCKET_KEY)))
                    .region(tsdb.getConfig().getString(configId(id, AWS_S3_REGION_KEY)))
                    .numShards(shardConfig.shardIds)
                    .awsCredentialsProvider(provider)
                    .withStatsCollector(tsdb.getStatsCollector())
                    .build();
    String duration = tsdb.getConfig().getString(
            configId(id, ConfigUtils.FLUSH_FREQUENCY_KEY));
    final long flushFrequency = DateTime.parseDuration(duration) / 1000;
    int segmentsInATimeSeries =
            storageMode.getSegmentsPerTimeseries(shardConfig.retentionHour, shardConfig.segmentSizeHour);
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(shardConfig.segmentSizeHour);
    return new MetaFlushImpl(
            new OffHeapTimeSeriesRecordFactory(segmentsInATimeSeries, secondsInASegment),
            new CompressedMetaWriterFactory(namespaces.get(0)),
            s3UploaderFactory,
            Executors.newWorkStealingPool(shardConfig.shardCount),
            tsdb.getStatsCollector(),
            namespaces.get(0),
            (int) flushFrequency);
  }

  private TSFlusher createTSFlusher(
          TimeSeriesEncoderFactory dsEncoderFactory,
          int secondsInASegment) {
    logger.info("Creating Time series flusher");
    List<LongTermStorage> stores = Lists.newArrayList();
    ASCluster cluster = new ASCluster(
            tsdb.getConfig().getString(configId(id, AS_HOSTS_KEY)), 3000);
    LTSAerospike longTermStore =
            new LTSAerospike(
                    cluster,
                    tsdb.getConfig().getString(configId(id, AS_NAMESPACE_KEY)),
                    6 * 3600, // TODO - config,
                    secondsInASegment,
                    shardConfig.namespace,
                    "b",
                    tsdb.getStatsCollector(),
                    metricsScheduler);
    stores.add(longTermStore);
    tsdb.getRegistry()
            .registerPlugin(
                    AerospikeClientPlugin.class, null, new AerospikeClientPlugin(longTermStore));
    String duration = tsdb.getConfig().getString(
            configId(id, ConfigUtils.FLUSH_FREQUENCY_KEY));
    final long flushFrequency = DateTime.parseDuration(duration);
    return new TSFlusherImp(
            timeSeriesRecordFactory,
            encoderFactory,
            dsEncoderFactory,
            shardConfig,
            stores,
            metricsScheduler,
            tsdb.getStatsCollector(),
            new String[] {"namespace", shardConfig.namespace, "shardId", "0"},
            flushFrequency,
            tsdb.getConfig().getInt(configId(id, FLUSH_THREADS_KEY)));
  }

}
