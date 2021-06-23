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

public class MatchAllQuery extends Query {

  private final int CLASS_NAME_HASH = getClass().getName().hashCode();

  MatchAllQuery(QueryBuilder builder) {
    super(builder);
  }
  
  public boolean equals(Object other) {
    return other != null && getClass() == other.getClass();
  }

  public int hashCode() {
    return CLASS_NAME_HASH;
  }

  @Override
  public String toString() {
    return "*";
  }

  public static class MetaMatchAllQuery extends MetaQuery {

    private final int CLASS_NAME_HASH = getClass().getName().hashCode();

    MetaMatchAllQuery(MetaQueryBuilder builder) {
      super(builder);
    }
    
    public boolean equals(Object other) {
      return other != null && getClass() == other.getClass();
    }

    public int hashCode() {
      return CLASS_NAME_HASH;
    }

    @Override
    public String toString() {
      return "*";
    }
  }
}
