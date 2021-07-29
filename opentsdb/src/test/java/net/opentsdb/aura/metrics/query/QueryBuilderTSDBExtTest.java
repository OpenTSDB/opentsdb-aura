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

package net.opentsdb.aura.metrics.query;

import net.opentsdb.aura.metrics.QueryBuilderTSDBExt;
import net.opentsdb.aura.metrics.meta.Filter;
import net.opentsdb.aura.metrics.meta.Query;
import net.opentsdb.aura.metrics.meta.QueryBuilder;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class QueryBuilderTSDBExtTest {

  @Test
  public void foobar() throws Exception {
        QueryFilter f =
            NotFilter.newBuilder()
                    .setFilter(TagValueRegexFilter.newBuilder()
                            .setKey("host")
                            .setFilter("foo")
                            .build())
                    .build();
    Query query = QueryBuilderTSDBExt.newBuilder()
            .fromTSDBQueryFilter(f)
            .build();
    System.out.println(query);
  }

  @Test
  public void foo() {
    QueryFilter f = ExplicitTagsFilter.newBuilder()
            .setFilter(ChainFilter.newBuilder()
                    .setOp(ChainFilter.FilterOp.AND)
                    .addFilter(TagValueLiteralOrFilter.newBuilder()
                            .setFilter("SUM")
                            .setKey("_aggregate")
                            .build())
                    .addFilter(ChainFilter.newBuilder()
                            .addFilter(TagValueLiteralOrFilter.newBuilder()
                                    .setFilter("Yamas|Foo")
                                    .setKey("namespace")
                                    .build())
                            .build())
                    .addFilter(TagValueRegexFilter.newBuilder()
                            .setKey("hostgroup")
                            .setFilter(".*")
                            .build())
                    .build())
            .build();

    QueryBuilder bldr = QueryBuilderTSDBExt.newBuilder()
            .fromTSDBQueryFilter(f);
    Query query = bldr.build();
    System.out.println(query);

  }

  @Test
  public void testWildcardFilter() {

    TagValueWildcardFilter wf =
        TagValueWildcardFilter.newBuilder().setKey("key").setFilter("VAL*").build();
    Query query = QueryBuilderTSDBExt.newBuilder().fromTSDBQueryFilter(wf).build();
    Filter auraFilter = query.getFilter();
    assertEquals(auraFilter.getOperator(), Filter.Operator.OR);
    assertEquals(auraFilter.getTagKey(), wf.getTagKey());
    assertEquals(auraFilter.getTagValues()[0], "VAL.*");
  }

}
