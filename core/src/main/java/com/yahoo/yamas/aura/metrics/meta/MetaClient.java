package com.yahoo.yamas.aura.metrics.meta;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

public interface MetaClient<ResT extends MetaTimeSeriesQueryResult> {

  /**
   * Blocking api
   */
  Iterator<ResT> getTimeseries(String query);

  /**
   * Async api
   */
  CountDownLatch getTimeseries(String query, StreamConsumer<ResT> consumer);

  interface StreamConsumer<T> {
    void onNext(T t);

    void onError(Throwable t);

    void onCompleted();
  }

}
