package uk.co.flax.luwak.matchers;
/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.search.Query;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.DocumentBatch;
import uk.co.flax.luwak.MatcherFactory;
import uk.co.flax.luwak.QueryMatch;

/**
 * Utility class to see how much time is taken in running the presearcher query.
 */
public class TimingsMatcher extends CandidateMatcher<QueryMatch> {

    public TimingsMatcher(DocumentBatch docs) {
        super(docs);
    }

    @Override
    public void doMatchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException {
        // no-op
    }

    @Override
    public QueryMatch resolve(QueryMatch match1, QueryMatch match2) {
        return match1;
    }

    public static MatcherFactory<QueryMatch> FACTORY = new MatcherFactory<QueryMatch>() {
        @Override
        public CandidateMatcher<QueryMatch> createMatcher(DocumentBatch doc) {
            return new TimingsMatcher(doc);
        }
    };

}
