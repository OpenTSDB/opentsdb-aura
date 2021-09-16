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

package net.opentsdb.aura.events.lucene.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

public class QueryParser {

    public static Query parse(Query query) {

        if (query instanceof TermQuery) {
            Term originalTerm = ((TermQuery) query).getTerm();
            Term term = new Term(originalTerm.field(), convertToRegex(originalTerm.text()));
            return new RegexpQuery(term);
        } else if (query instanceof WildcardQuery) {
            Term originalTerm = ((WildcardQuery) query).getTerm();
            Term term = new Term(originalTerm.field(), convertToRegex(originalTerm.text()));
            return new RegexpQuery(term);
        } else if (query instanceof PrefixQuery) {
            Term originalTerm = ((PrefixQuery) query).getPrefix();
            Term term = new Term(originalTerm.field(), convertToRegex(originalTerm.text()));
            return new RegexpQuery(term);
        } else if (query instanceof BooleanQuery) {
            BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
            for (BooleanClause clause : ((BooleanQuery) query).clauses()) {
                booleanQueryBuilder.add(parse(clause.getQuery()), clause.getOccur());
            }
            return booleanQueryBuilder.build();
         }else if (query instanceof RegexpQuery) {
            Term originalTerm = ((RegexpQuery) query).getRegexp();
            Term term = new Term(originalTerm.field(), convertToRegex(originalTerm.text()));
            return new RegexpQuery(term);
        } else {
            return query;
        }
    }

    public static String convertToRegex(String text) {
        StringBuilder sb = new StringBuilder();
        sb.append(".*");
        sb.append(text);
        sb.append(".*");
        return sb.toString();
    }
}
