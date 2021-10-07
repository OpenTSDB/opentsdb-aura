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

package net.opentsdb.aura.metrics.storage;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.core.downsample.AggregatorType;
import net.opentsdb.aura.metrics.core.downsample.Interval;
import net.opentsdb.aura.metrics.core.downsample.SegmentWidth;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.MergedMetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.grpc.MetaGrpcClient;
import net.opentsdb.common.Const;
import net.opentsdb.data.ArrayAggregatorConfig;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.groupby.GroupByFactory;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class AerospikeBatchQueryNode extends AbstractQueryNode implements TimeSeriesDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(AerospikeBatchQueryNode.class);

  protected final GroupByFactory groupByFactory;
  protected final LTSAerospike client;
  protected final String metricString;
  protected TimeSeriesDataSourceConfig config;
  protected boolean queryIncludesNamespace;

  protected int batchLimit;
  protected int threads;
  protected NumericArrayAggregatorFactory aggregatorFactory;
  protected ArrayAggregatorConfig aggConfig;
  protected GroupByConfig groupByConfig;
  protected DownsampleConfig downsampleConfig;
  protected RateConfig rateConfig;
  protected ArrayObjectPool doublePool;
  protected int segmentsStart;
  protected int segmentsEnd;
  protected final int secondsInSegment;
  protected final int gbQueueThreshold;
  protected final int seriesPerJob;
  protected final int gbThreads;
  protected String[] gbKeys;
  protected QR qr;
  protected MetaTimeSeriesQueryResult metaResult;
  List<AerospikeBatchJob> jobs = Lists.newArrayList();

  private Set<String> aggsInStore;
  private boolean useRollupData;
  private SegmentWidth rollupSegmentWidth;
  private Interval rollupInterval;
  private int rollupIntervalCount;
  private byte rollupAggId;
  private int rollupAggOrdinal;

  public AerospikeBatchQueryNode(final AerospikeBatchSourceFactory factory,
                                 final QueryPipelineContext context,
                                 final TimeSeriesDataSourceConfig config,
                                 final boolean queryIncludesNamespace) {
    super(factory, context);
    this.config = config;
    this.queryIncludesNamespace = queryIncludesNamespace;
    qr = new QR();

    metricString = config.getMetric().getMetric();
    final AerospikeClientPlugin clientPlugin = context.tsdb().getRegistry().getDefaultPlugin(AerospikeClientPlugin.class);
    if (clientPlugin == null) {
      throw new IllegalStateException("No plugin for the AS Client?");
    }
    client = clientPlugin.asClient();

    secondsInSegment = pipelineContext().tsdb().getConfig().getInt(AerospikeBatchSourceFactory.SECONDS_IN_SEGMENT_KEY);
    groupByFactory = (GroupByFactory) pipelineContext().tsdb().getRegistry().getQueryNodeFactory(GroupByFactory.TYPE);
    gbQueueThreshold = pipelineContext().tsdb().getConfig().getInt(GroupByFactory.GROUPBY_QUEUE_THRESHOLD_KEY);
    seriesPerJob = pipelineContext().tsdb().getConfig().getInt(GroupByFactory.GROUPBY_TIMESERIES_PER_JOB_KEY);
    gbThreads = pipelineContext().tsdb().getConfig().getInt(GroupByFactory.GROUPBY_THREAD_COUNT_KEY);
    batchLimit = pipelineContext().tsdb().getConfig().getInt(AerospikeBatchSourceFactory.AS_BATCH_LIMIT_KEY);
    threads = pipelineContext().tsdb().getConfig().getInt(AerospikeBatchSourceFactory.AS_JOBS_PER_QUERY);
    doublePool = (ArrayObjectPool) pipelineContext().tsdb().getRegistry().getObjectPool(DoubleArrayPool.TYPE);
    String aggs = pipelineContext().tsdb().getConfig().getString(AerospikeBatchSourceFactory.AS_ROLLUP_AGGS);
    aggsInStore = new HashSet<>();
    if(aggs != null && !aggs.isEmpty()) {
      String[] split = aggs.split(",");
      for (String agg : split) {
        aggsInStore.add(agg);
      }
      this.rollupSegmentWidth = SegmentWidth.valueOf(pipelineContext().tsdb().getConfig().getString(AerospikeBatchSourceFactory.AS_ROLLUP_SEGMENT_WIDTH));
      this.rollupInterval = Interval.valueOf(pipelineContext().tsdb().getConfig().getString(AerospikeBatchSourceFactory.AS_ROLLUP_INTERVAL));
      this.rollupIntervalCount = rollupInterval.getCount(rollupSegmentWidth);
    }
  }

  @Override
  public Deferred<Void> initialize(Span span) {
    return super.initialize(span)
            .addCallback(
                    arg -> {
                      if (config.getPushDownNodes() != null) {
                        for (QueryNodeConfig queryNodeConfig :
                                (List<QueryNodeConfig>) config.getPushDownNodes()) {
                          if (queryNodeConfig instanceof DownsampleConfig) {
                            downsampleConfig = (DownsampleConfig) queryNodeConfig;
                          } else if (queryNodeConfig instanceof GroupByConfig) {
                            groupByConfig = (GroupByConfig) queryNodeConfig;
                          } else if (queryNodeConfig instanceof RateConfig) {
                            rateConfig = (RateConfig) queryNodeConfig;
                          }
                        }
                      }

                      if (groupByConfig == null) {
                        LOGGER.info("No group by config");
                        // return Deferred.fromError(new IllegalStateException("No group by config
                        // found."));
                      } else {
                        aggregatorFactory =
                                pipelineContext()
                                        .tsdb()
                                        .getRegistry()
                                        .getPlugin(
                                                NumericArrayAggregatorFactory.class, groupByConfig.getAggregator());
                        if (groupByConfig.getTagKeys() != null) {
                          gbKeys = new String[groupByConfig.getTagKeys().size()];
                          int idx = 0;
                          for (String key : groupByConfig.getTagKeys()) {
                            gbKeys[idx++] = key;
                          }
                        }
                      }

                      if (downsampleConfig != null) {
                        aggConfig =
                                DefaultArrayAggregatorConfig.newBuilder()
                                        .setArraySize(downsampleConfig.intervals())
                                        //.setInfectiousNaN(groupByConfig.getInfectiousNan()) // TODO - fix GB nan
                                        .build();
                        String agg = downsampleConfig.getAggregator();
                        useRollupData = aggsInStore.contains(agg);
                        if (useRollupData) {
                          AggregatorType aggregatorType = AggregatorType.valueOf(agg);
                          this.rollupAggId = aggregatorType.getId();
                          this.rollupAggOrdinal = aggregatorType.getOrdinal();
                          int dsInterval = (int) downsampleConfig.interval().get(ChronoUnit.SECONDS);
                          int rollupInterval = this.rollupInterval.getSeconds();
                          int segmentWidth = rollupSegmentWidth.getSeconds();
                          if (dsInterval < rollupInterval) {
                            throw new IllegalArgumentException(
                                "Downsample interval: "
                                    + dsInterval
                                    + " is less than rollup interval: "
                                    + rollupInterval);
                          }
                          if(segmentWidth % rollupInterval != 0) {
                            throw new IllegalArgumentException(
                                "Rollup interval: "
                                    + rollupInterval
                                    + " should be a multiplier of the segment width : "
                                    + segmentWidth);
                          }
                        }
                      }

                      // compute query timestamps.
                      segmentsStart = (int) config.startTimestamp().epoch();
                      segmentsStart = segmentsStart - (segmentsStart % client.secondsInRecord());

                      segmentsEnd = (int) config.endTimestamp().epoch();
                      segmentsEnd = segmentsEnd - (segmentsEnd % client.secondsInRecord());
                      if (segmentsEnd < (int) config.endTimestamp().epoch()) {
                        segmentsEnd += client.secondsInRecord(); // always exclusive and may be in the future.
                      }

                      LOGGER.info("Instantiated AS query node from {} to {}", segmentsStart, segmentsEnd);
                      LOGGER.info(
                              "Has GB {} Has DS {} Has Rate {}", (groupByConfig != null), (downsampleConfig != null), (rateConfig != null));
                      return null;
                    });
  }

  @Override
  public void fetchNext(Span span) {
    Span child = null;
    if (span != null) {
      child = span.newChild(AerospikeQueryNode.class.getName()).start();
    }

    String temp = config.getMetric().getMetric();
    String namespace = null;
    if (queryIncludesNamespace && Strings.isNullOrEmpty(config.getNamespace())) {
      final int dotIndex = temp.indexOf(".");
      if (dotIndex >= 0) {
        // strip out the namespace
        namespace = temp.substring(0, temp.indexOf("."));
        temp = temp.substring(temp.indexOf(".") + 1);
      }
    }

    String metric = temp;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Query metric: {}", metric);
    }

    QueryFilter queryFilter = config.getFilter();
    if (queryFilter == null && !Strings.isNullOrEmpty(config.getFilterId())) {
      queryFilter = pipelineContext().query().getFilter(config.getFilterId());
    }

    TimeSeriesQuery semanticQuery = pipelineContext().query();
    int start;
    int end;
    if (config.timeShifts() != null) {
      TimeStamp ts = config.startTimestamp().getCopy();
      ts.subtract((TemporalAmount) config.timeShifts().getValue());
      start = (int) ts.epoch();

      ts = config.endTimestamp().getCopy();
      ts.subtract((TemporalAmount) config.timeShifts().getValue());
      end = (int) ts.epoch();
    } else {
      start = (int) config.startTimestamp().epoch();
      end = (int) config.endTimestamp().epoch();
    }

    long mystStart = System.nanoTime();

    String metaQuery = MystQueryBuilder.build(namespace, queryFilter, metric, start, end, gbKeys);
    LOGGER.info("Sending meta query... {}", metaQuery);
    context.queryContext().logTrace(this, "Sending query to Myst: " + metaQuery);
    MetaGrpcClient metaGrpcClient = ((AerospikeBatchSourceFactory) factory).metaGrpcClient();
    MetaTimeSeriesQueryResult mmtqr = new MergedMetaTimeSeriesQueryResult();
    Iterator<DefaultMetaTimeSeriesQueryResult> metaIterator = metaGrpcClient.getTimeseries(metaQuery);
    long mergeStart = System.nanoTime();
    while (metaIterator.hasNext()) {
      DefaultMetaTimeSeriesQueryResult dmtqr = metaIterator.next();
      ((MergedMetaTimeSeriesQueryResult) mmtqr).add(dmtqr);
      LOGGER.info("Got a response from meta! Groups: {} Total: {}", dmtqr.numGroups(), dmtqr.totalHashes());
    }
    long mergeTime = System.nanoTime() - mergeStart;
    double timeTaken = DateTime.msFromNanoDiff(System.nanoTime(), mystStart);
    context.queryContext().logTrace(this, "Myst time taken " + timeTaken + "ms merge time: " +
            ((double) mergeTime / (double) 1_000_000) + "ms");
    context.queryContext().logTrace(this, "Total Myst Groups: " + mmtqr.numGroups());
    context.queryContext().logTrace(this, "Total Myst Hashes: " + mmtqr.totalHashes());
    LOGGER.info("Final Meta: {} groups {} total hashes {}ms merge {}ms", mmtqr.numGroups(), mmtqr.totalHashes(), timeTaken,
                ((double) mergeTime / (double) 1_000_000));
    metaResult = mmtqr;

    // TEMP flush to file code
    if (false) {
      try {
        long qhash = queryFilter.buildHashCode().asLong();
        FileOutputStream fos = new FileOutputStream("meta_" + metric + "_" + qhash + "_" + (System.currentTimeMillis() / 1000) + ".json");
        JsonGenerator json = JSON.getFactory().createGenerator(fos);
        json.writeStartObject();
        json.writeArrayFieldStart("groups");
        Map<Long, String> dict = Maps.newHashMap();
        for (int i = 0; i < metaResult.numGroups(); i++) {
          MetaTimeSeriesQueryResult.GroupResult gr = metaResult.getGroup(i);
          json.writeStartObject();
          json.writeArrayFieldStart("tags");
          MetaTimeSeriesQueryResult.GroupResult.TagHashes hashes = gr.tagHashes();
          for (int x = 0; x < hashes.size(); x++) {
            long hash = hashes.next();
            json.writeNumber(hash);
            if (!dict.containsKey(hash)) {
              String str = metaResult.getStringForHash(hash);
              dict.put(hash, str);
            }
          }
          json.writeEndArray();
          json.writeArrayFieldStart("hashes");
          for (int x = 0; x < gr.numHashes(); x++) {
            json.writeNumber(gr.getHash(x));
          }
          json.writeEndArray();
          json.writeEndObject();
        }
        json.writeEndArray();

        // dict
        json.writeObjectFieldStart("dict");
        for (Map.Entry<Long, String> entry : dict.entrySet()) {
          json.writeNumberField(entry.getValue(), entry.getKey());
        }
        json.writeEndObject();

        json.writeArrayFieldStart("gbTags");
        if (gbKeys != null) {
          for (int i = 0; i < gbKeys.length; i++) {
            json.writeString(gbKeys[i]);
          }
        }
        json.writeEndArray();

        json.writeEndObject();

        json.flush();
        json.close();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

      if (pipelineContext().queryContext().isClosed()) {
        return;
      }

    if (metaResult.totalHashes() > 0) {
      long asStart = System.nanoTime();
      try {
        startBatches().await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      long totalBatchTime = System.nanoTime() - asStart;
      LOGGER.info("Total batch time: {}ms", ((double) totalBatchTime / (double) 1_000_000));
        if (context.query().isTraceEnabled()) {
          context.queryContext().logTrace("Total AS batch job time: " +
                  ((double) totalBatchTime / (double) 1_000_000) + "ms");
        }

      // MERGE results
      mergeStart = System.nanoTime();
      TLongObjectMap<AggregatorFinishedTS> map = jobs.get(0).groups;

        // stats
        AerospikeBatchJob job = jobs.get(0);
        long totalAStime = job.totalAStime;
        long maxASTime = job.totalAStime;
        long totalDecodeTime = job.totalDecodeTime;
        long maxDecodeTime = job.totalDecodeTime;
        int totalRuns = job.totalRuns;
        int keyHits = job.keyHits;

      for (int i = 1; i < jobs.size(); i++) {
          job = jobs.get(i);
          TLongObjectMap localMap = job.groups;
        TLongObjectIterator<AggregatorFinishedTS> iterator = localMap.iterator();
        while (iterator.hasNext()) {
          iterator.advance();
          long group = iterator.key();

          AggregatorFinishedTS extant = map.get(group);
          if (extant != null) {
            extant.combine(iterator.value());
            iterator.value().close();
          } else {
            map.put(iterator.key(), iterator.value());
          }
        }

          totalAStime += job.totalAStime;
          if (job.totalAStime > maxASTime) {
            maxASTime = job.totalAStime;
          }
          totalDecodeTime += job.totalDecodeTime;
          if (job.totalDecodeTime > maxDecodeTime) {
            maxDecodeTime = job.totalDecodeTime;
          }
          totalRuns += job.totalRuns;
          keyHits += job.keyHits;
          job.close();
      }

      long mergeTimeTaken = System.nanoTime() - mergeStart;
        LOGGER.debug("AS Result merge: {}ms", ((double) mergeTimeTaken / (double) 1_000_000));
        if (context.query().isTraceEnabled()) {
          context.queryContext().logTrace("Total result merge time: " +
                  ((double) mergeTimeTaken / (double) 1_000_000) + "ms");
          context.queryContext().logTrace("Avg AS fetch time per batch: " +
                  (((double) totalAStime / (double) 1_000_000) / (double) jobs.size()) + "ms. Max " +
                  ((double) maxASTime / (double) 1_000_000) + "ms");
          context.queryContext().logTrace("Avg segment decode time per batch: " +
                  (((double) totalDecodeTime / (double) 1_000_000) / (double) jobs.size()) + "ms. Max " +
                  ((double) maxDecodeTime / (double) 1_000_000) + "ms");
          context.queryContext().logTrace("Total batch job runs: " + totalRuns);
          context.queryContext().logTrace("Total batch jobs: " + jobs.size());
          context.queryContext().logTrace("Total key hits: " + keyHits);
        }

      qr.setup(map);
        job.close();

      sendUpstream(qr);
      return;
    }
    // no data!
    qr.setup(null);
    sendUpstream(qr);
  }

  @Override
  public String[] setIntervals() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {}

  @Override
  public void onNext(QueryResult queryResult) {
    sendUpstream(queryResult);
  }

  public String[] gbKeys() {
    return gbKeys;
  }

  public GroupByConfig groupByConfig() {
    return groupByConfig;
  }

  public DownsampleConfig downsampleConfig() {
    return downsampleConfig;
  }

  public RateConfig rateConfig() {
    return rateConfig;
  }

  public NumericArrayAggregator newAggregator() {
    return (NumericArrayAggregator) aggregatorFactory.newAggregator(aggConfig);
  }

  public LTSAerospike asClient() {
    return client;
  }

  public int getSegmentsStart() {
    return segmentsStart;
  }

  public int getSegmentsEnd() {
    return segmentsEnd;
  }

  public int getSecondsInSegment() {
    return secondsInSegment;
  }

  public boolean useRollupData() {
    return useRollupData;
  }

  public Interval getRollupInterval() {
    return rollupInterval;
  }

  public int getRollupIntervalCount() {
    return rollupIntervalCount;
  }

  public byte getRollupAggId() {
    return rollupAggId;
  }

  public int getRollupAggOrdinal() {
    return rollupAggOrdinal;
  }

  public ArrayObjectPool getDoublePool() {
    return doublePool;
  }

  CountDownLatch startBatches() {
    final ObjectPool batchPool = pipelineContext().tsdb().getRegistry().getObjectPool(AerospikeBatchJobAllocator.TYPE);
    int recordsPerHash = (segmentsEnd - segmentsStart) / client.secondsInRecord();
    int hashesPerJob = Math.max(batchLimit,
            (int) Math.ceil((double) metaResult.totalHashes() / (double) threads));
    hashesPerJob = Math.max(1, hashesPerJob / recordsPerHash);
    int threadsUsed = Math.min(threads, (int) Math.ceil((double) metaResult.totalHashes() / (double) hashesPerJob));
    // should only happen once.
    while (((double) metaResult.totalHashes() / (double) hashesPerJob) > threadsUsed) {
      // need to bump up the hash number
      hashesPerJob++;
      threadsUsed = Math.min(threads, (int) Math.ceil((double) metaResult.totalHashes() / (double) hashesPerJob));
    }

    int curHashes = 0;
    CountDownLatch latch;
    latch = new CountDownLatch(threadsUsed);
    AerospikeBatchJob job;
    if (batchPool != null) {
      job = (AerospikeBatchJob) batchPool.claim().object();
    } else {
      job = new AerospikeBatchJob();
    }
    job.reset(this, metaResult, latch);
    for (int i = 0; i < metaResult.numGroups(); i++) {
      final int numHashes = metaResult.getGroup(i).numHashes();
      if (curHashes + numHashes >= hashesPerJob) {
        // new job
        int delta = hashesPerJob - curHashes;
        job.endGroupId = i;

        int newJobHashIdx = job.endGroupId == job.startGroupId ?
                job.startHashId + delta : delta;
        job.endHashId = newJobHashIdx;
        jobs.add(job);
        context.tsdb().getQueryThreadPool().submit(job, context.queryContext());

        // we could have a ton of data in one group, so check for that
        int remaining = numHashes - delta;
        while (remaining >= hashesPerJob) {
          job = new AerospikeBatchJob();
          job.reset(this, metaResult, latch);
          job.startGroupId = i;
          job.startHashId = newJobHashIdx;
          job.endGroupId = i;
          job.endHashId = job.startHashId + hashesPerJob;
          remaining -= hashesPerJob;
          newJobHashIdx += hashesPerJob;
          jobs.add(job);
          context.tsdb().getQueryThreadPool().submit(job, context.queryContext());
        }

        // handle leftovers
        curHashes = remaining;
        job = new AerospikeBatchJob();
        job.reset(this, metaResult, latch);
        if (curHashes == 0 || newJobHashIdx >= numHashes) {
          job.startGroupId = i + 1;
          job.startHashId = 0;
        } else {
          job.startGroupId = i;
          job.startHashId = newJobHashIdx;
        }
      } else {
        curHashes += numHashes;
      }
    }

    // remainder
    if (curHashes > 0) {
      job.endGroupId = metaResult.numGroups() - 1;
      job.endHashId = metaResult.getGroup(job.endGroupId).numHashes();
      jobs.add(job);
      context.tsdb().getQueryThreadPool().submit(job, context.queryContext());
    }
    return latch;
  }

  public class QR implements QueryResult, TimeSpecification {

    List<TimeSeries> results;
    void setup(TLongObjectMap<AggregatorFinishedTS> map) {
      if (map != null) {
        results = Lists.newArrayList(map.valueCollection());
      } else {
        results = Collections.emptyList();
      }
      LOGGER.info("Final results: {}", results.size());
    }

    public String metricString() {
      return metricString;
    }

    @Override
    public TimeSpecification timeSpecification() {
      return this;
    }

    @Override
    public List<TimeSeries> timeSeries() {
      return results;
    }

    @Override
    public String error() {
      return null;
    }

    @Override
    public Throwable exception() {
      return null;
    }

    @Override
    public long sequenceId() {
      return 0;
    }

    @Override
    public QueryNode source() {
      return AerospikeBatchQueryNode.this;
    }

    @Override
    public QueryResultId dataSource() {
      return (QueryResultId) config.resultIds().get(0);
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }

    @Override
    public ChronoUnit resolution() {
      return ChronoUnit.SECONDS;
    }

    @Override
    public RollupConfig rollupConfig() {
      return ((TimeSeriesDataSourceFactory) ((AbstractQueryNode) AerospikeBatchQueryNode.this).factory()).rollupConfig();
    }

    @Override
    public void close() {
      for (int i = 0; i < results.size(); i++) {
        results.get(i).close();
      }
    }

    @Override
    public boolean processInParallel() {
      return true;
    }

    @Override
    public TimeStamp start() {
      return downsampleConfig.startTime();
    }

    @Override
    public TimeStamp end() {
      return downsampleConfig.endTime();
    }

    @Override
    public TemporalAmount interval() {
      return downsampleConfig.interval();
    }

    @Override
    public String stringInterval() {
      return downsampleConfig.getInterval();
    }

    @Override
    public ChronoUnit units() {
      return downsampleConfig.units();
    }

    @Override
    public ZoneId timezone() {
      return downsampleConfig.timezone();
    }

    @Override
    public void updateTimestamp(int offset, TimeStamp timestamp) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException();
    }

    @Override
    public void nextTimestamp(TimeStamp timestamp) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException();
    }

  }

  int segmentsStart() {
    return segmentsStart;
  }

  int segmentsEnd() {
    return segmentsEnd;
  }

  int secondsInRecord() {
    return client.secondsInRecord();
  }

  LTSAerospike client() {
    return client;
  }

  String metricString() {
    return metricString;
  }

  String getStringForHash(final long hash) {
    return metaResult.getStringForHash(hash);
  }

  public QR queryResult() {
    return qr;
  }

  int batchLimit() {
    return batchLimit;
  }

  void submit(Runnable runnable) {
    context.tsdb().getQueryThreadPool().submit(runnable, context.queryContext());
  }
}