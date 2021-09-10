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

package net.opentsdb.servlet.resources;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import net.opentsdb.ConfigUtils;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.stats.BlackholeTracer;
import net.opentsdb.stats.DefaultQueryStats;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static net.opentsdb.ConfigUtils.configId;

/**
 * A health endpoint to use for TSDs determining whether or not to query
 * the instance.
 */
@Path("health")
public class HealthResource extends BaseTSDBPlugin implements ServletResource {
  private static Logger LOG = LoggerFactory.getLogger(HealthResource.class);
  private static final String TYPE = "HealthResource";

  private static final String OVERRIDE_KEY = "metrics.health.override";
  private static final String HEARTBEAT_QUERY_KEY = "metrics.health.heartbeat.query";
  private static final String FREQUENCY_KEY = "metrics.health.frequency";
  private static final TypeReference<SemanticQuery> QUERY_TYPE_REFERENCE =
          new TypeReference<SemanticQuery>() {};

  private long start_time_epoch;


  private static final Object MUTEX = new Object();
  private volatile List<Map<String, Long>> namespaceHeartMap = null;
  private volatile byte[] healthInfo = null;

  private long retentionSeconds;// = AuraConfig.getRetentionHour() * 60 * 60 + (10 * 60);

  private long updateFrequency;
  private Thread healthFetchThread;
  private List<String> namespaces;

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> shutdown() {
    if (healthFetchThread != null) {
      healthFetchThread.interrupt();
    }
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = Strings.isNullOrEmpty(id) ? "HealthResource" : id;

    // TEMP - should happen in main
    ConfigUtils.registerConfigs(tsdb, null);
    start_time_epoch = DateTime.currentTimeMillis() / 1000;
    final String retention = tsdb.getConfig().getString(
            configId(null, ConfigUtils.RETENTION_KEY));
    retentionSeconds = DateTime.parseDuration(retention) / 1000;

    namespaces = tsdb.getConfig().getTyped(
            configId(null, ConfigUtils.NAMESPACES_KEY),
            ConfigUtils.STRING_LIST);

    if (!tsdb.getConfig().hasProperty(configId(null, OVERRIDE_KEY))) {
      tsdb.getConfig().register(configId(null, OVERRIDE_KEY),
              "ENABLE",
              true,
              "One of: 'ENABLE' to query always, 'DISABLE' to never query "
                      + "and 'AUTO' to compute the health.");
    }

    if (!tsdb.getConfig().hasProperty(configId(null, HEARTBEAT_QUERY_KEY))) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
                      .setKey(configId(null, HEARTBEAT_QUERY_KEY))
                      .setType(QUERY_TYPE_REFERENCE)
                      .setDescription("A Semantic query for heartbeat data used " +
                              "to determine if there is enough data present to " +
                              "satisfy real queries.")
                      .isNullable()
                      .isDynamic()
                      .setSource(getClass().getName())
                      .build());
    }

    if (!tsdb.getConfig().hasProperty(configId(null, FREQUENCY_KEY))) {
      tsdb.getConfig().register(configId(null, FREQUENCY_KEY),
              "1m",
              false,
              "How often to run the heartbeat query. A TSDB duration");
    }

    final String duration = tsdb.getConfig().getString(
            configId(null, FREQUENCY_KEY));
    if (!Strings.isNullOrEmpty(duration)) {
      updateFrequency = DateTime.parseDuration(duration);
      startHealthCheckThread();
    }

    return Deferred.fromResult(null);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response check(final @Context ServletConfig servlet_config,
                        final @Context HttpServletRequest request) {
    if (healthInfo == null) {
      LOG.warn("No health info yet.");
      return Response.ok("{}").build();
    } else {
      return Response.ok(healthInfo).build();
    }
  }

  @Path("{namespace}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response checkNamespace(final @PathParam("namespace") String namespace,
                                 final @Context ServletConfig servlet_config,
                                 final @Context HttpServletRequest request) {
    try {
      List<Map<String, Long>> results = this.namespaceHeartMap;
      if (results == null || results.isEmpty()) {
        // nothing in yet. The thread hasn't issued a query or we didn't have
        // heartbeat data.
        return Response.ok("{}").build();
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      JsonGenerator generator = JSON.getFactory().createGenerator(baos);
      generator.writeStartObject();
      int i = 0;
      for (final Map<String, Long> result : results) {
        if (result == null) {
          continue;
        }
        if (results.size() > 1 || namespace.equals("*")) {
          generator.writeObjectFieldStart(namespaces.get(i++));
        } else {
          generator.writeObjectFieldStart(namespace);
        }
        generator.writeNumberField("earliest", result.get("earliest"));
        generator.writeNumberField("latest", result.get("latest"));
        generator.writeNumberField("count", result.get("count"));
      }
      generator.writeEndObject();
      generator.close();

      return Response.ok(baos.toByteArray()).build();
    } catch (Exception e) {
      throw new WebApplicationException("Unexpected exception.", e,
          Response.Status.INTERNAL_SERVER_ERROR);
    }

  }

  private void startHealthCheckThread() {
    healthFetchThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  Thread.sleep(updateFrequency);
                  namespaceHeartMap = fetchHB();
                  healthInfo = buildHealthInfo();
                } catch (InterruptedException e) {
                  return;
                } catch (Throwable t) {
                  LOG.error("Exception building health info", t);
                }
              }
            });
    healthFetchThread.start();
  }

  private List<Map<String, Long>> fetchHB() {
    final List<Deferred<Map<String, Long>>> deferreds = Lists.newArrayList();
    deferreds.add(namespaceHeartbeat());
    try {
      return Deferred.groupInOrder(deferreds).join(30_000);
    } catch (Exception e) {
      LOG.error("Error reading heartbeat", e);
    }
    return null;
  }

  private byte[] buildHealthInfo() {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      JsonGenerator generator = JSON.getFactory().createGenerator(baos);
      generator.writeStartObject();

      generator.writeStringField("override", tsdb.getConfig().getString(
              configId(null, OVERRIDE_KEY)));

      generator.writeNumberField("uptimeSeconds",
          (DateTime.currentTimeMillis() / 1000) - start_time_epoch);

      // TODO - figure out how to plug these in and track them across shards.
//      generator.writeNumberField("totalKafkaRead", KafkaMetricConsumer.totalEventCount.get());
//      generator.writeNumberField("eventReadRate", Main.eventReadRate);
//      generator.writeNumberField("totalTimeSeries", TimeSeriesShard.totalTimeSeriesCount.get());
//      generator.writeNumberField("newTimeSeriesWriteRate", Main.tsWriteRate);
//      generator.writeNumberField("duplicateTimeSeries", TimeSeriesShard.duplicateTimeSeriesCount.get());
//      generator.writeNumberField("duplicateTimeSeriesRate", Main.dpulicateTSRate);
//      generator.writeNumberField("uniqueTimeSeries", TimeSeriesShard.uniqTimeSeriesCount.get());
//      generator.writeNumberField("segments", TimeSeriesShard.totalSegmentCount.get());
//      generator.writeNumberField("totalDroppedTS", TimeSeriesShard.droppedTimeSeriesCount.get());
//      generator.writeNumberField("droppedTSRate", Main.droppedTSRate);

//      if (offset_recorder != null) {
//        final List<Pair<Long, Long>> history = offset_recorder.getHistory();
//        generator.writeArrayFieldStart("lagHistory");
//        for (final Pair<Long, Long> lag : history) {
//          generator.writeStartObject();
//          generator.writeNumberField("timestamp", lag.getKey());
//          generator.writeNumberField("lag", lag.getValue());
//          generator.writeEndObject();
//        }
//        generator.writeEndArray();
//
//        // trend
//        if (history.size() > 0) {
//          double[] rates = new double[history.size() - 1];
//          for (int i = 1; i < history.size(); i++) {
//            rates[i - 1] =
//                (history.get(i).getValue() - history.get(i - 1).getValue()) /
//                    (history.get(i).getKey() - history.get(i - 1).getKey());
//          }
//          generator.writeArrayFieldStart("lagRate");
//          for (final double rate : rates) {
//            generator.writeNumber(rate);
//          }
//          generator.writeEndArray();
//
//          // weighted average to give more emphasis on the recent lags
//          double avg = 0;
//          int divisor = 0;
//          for (int i = 0; i < rates.length; i++) {
//            avg += (rates[i] * (i + 1));
//            divisor += i + 1;
//          }
//
//          generator.writeNumberField("weightedAverageLagRate",
//              (avg / (double) divisor));
//        }
//      }
      generator.close();

    } catch (Exception e) {
      LOG.error("Error building health info", e);
    }
    return baos.toByteArray();
  }

  Deferred<Map<String, Long>> namespaceHeartbeat() {
    SemanticQuery query = tsdb.getConfig().getTyped(
            configId(null, HEARTBEAT_QUERY_KEY),
            QUERY_TYPE_REFERENCE);
    if (query == null) {
      return Deferred.fromResult(null);
    }

    final long now = DateTime.currentTimeMillis() / 1000;
    query = query.toBuilder()
            .setStart(Long.toString(now - retentionSeconds))
            .setEnd(Long.toString(now))
            .build();

    final Deferred<Map<String, Long>> deferred =
        new Deferred<Map<String, Long>>();

    class HealthSink implements QuerySink {

      private Map<String, Long> results;

      @Override
      public void onComplete() {
        deferred.callback(results);
      }

      @Override
      public void onNext(QueryResult next) {
        if (next.timeSeries() == null || next.timeSeries().isEmpty()) {
          next.close();
          return;
        }

        final TimeSeries ts = next.timeSeries().iterator().next();
        final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
            ts.iterator(NumericArrayType.TYPE);
        if (!optional.isPresent()) {
          next.close();
          return;
        }

        final TypedTimeSeriesIterator iterator = optional.get();
        if (!iterator.hasNext()) {
          next.close();
          return;
        }

        results = new HashMap<>();
        TimeSeriesValue<NumericArrayType> value =
            (TimeSeriesValue<NumericArrayType>) iterator.next();
        final TimeStamp time = next.timeSpecification().start().getCopy();
        long count = 0;
        if (value.value().isInteger()) {
          for (int i = value.value().offset(); i < value.value().end(); i++) {
            if (count++ == 0) {
              results.put("earliest", time.epoch());
            }
            time.add(next.timeSpecification().interval());
          }
        } else {
          for (int i = value.value().offset(); i < value.value().end(); i++) {
            if (!Double.isNaN(value.value().doubleArray()[i])) {
              if (count++ == 0) {
                results.put("earliest", time.epoch());
              }
            }
            time.add(next.timeSpecification().interval());
          }
        }
        results.put("latest", time.epoch());
        results.put("count", count);
        next.close();
      }

      @Override
      public void onNext(PartialTimeSeries partialTimeSeries,
                         QuerySinkCallback querySinkCallback) {
        throw new UnsupportedOperationException("TODO");
      }

      @Override
      public void onError(final Throwable t) {
        deferred.callback(t);
      }

    }

    final BlackholeTracer tracer = new BlackholeTracer();
    QueryContext context = SemanticQueryContext.newBuilder()
        .setTSDB(tsdb)
        .setMode(query.getMode())
        .setQuery(query)
        .addSink(new HealthSink())
        .setStats(DefaultQueryStats.newBuilder()
            .setTrace(tracer)
            .setQuerySpan(tracer.firstSpan())
            .build())
        .build();
    try {
      context.initialize(tracer.firstSpan()).join();
    } catch (InterruptedException e) {
      LOG.error("Interrupted!", e);
      throw new RuntimeException("WTF?", e);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      throw new RuntimeException("WTF?", e);
    }
    context.fetchNext(tracer);
    return deferred;
  }

}
