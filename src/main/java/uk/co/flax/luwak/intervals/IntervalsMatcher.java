package uk.co.flax.luwak.intervals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import uk.co.flax.luwak.CandidateMatcher;
import uk.co.flax.luwak.DocumentBatch;
import uk.co.flax.luwak.MatcherFactory;

/**
 * Copyright (c) 2014 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * CandidateMatcher class that will return exact hit positions for all matching queries
 *
 * If a stored query does not support interval iterators, an IntervalsQueryMatch object
 * with no Hit positions will be returned.
 *
 * If a query is matched, it will be run a second time against the highlight query (if
 * not null) to get positions.
 */
public class IntervalsMatcher extends CandidateMatcher<IntervalsQueryMatch> {

    public IntervalsMatcher(DocumentBatch docs) {
        super(docs);
    }

    @Override
    public IntervalsQueryMatch resolve(IntervalsQueryMatch match1, IntervalsQueryMatch match2) {
        return IntervalsQueryMatch.merge(match1.getQueryId(), match1.getDocId(), match1, match2);
    }

    @Override
    public void matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException {

        final QueryIntervalsMatchCollector collector = new QueryIntervalsMatchCollector(queryId, docs);
        docs.getSearcher().search(matchQuery, collector);

        // If there are no hits, we're done
        if (collector.getMatches().size() == 0)
            return;

        // If there's no highlight query, then we just add all matches and return
        if (highlightQuery == null) {
            for (IntervalsQueryMatch match : collector.getMatches()) {
                addMatch(match);
            }
            return;
        }

        // We need to run the highlight query against all the matching docs
        QueryIntervalsMatchCollector collector2 = new QueryIntervalsMatchCollector(queryId, docs);
        Filter matchesFilter = new Filter() {
            @Override
            public DocIdSet getDocIdSet(LeafReaderContext leafReaderContext, Bits bits) throws IOException {
                return collector.getMatchBitset();
            }
        };
        docs.getSearcher().search(highlightQuery, matchesFilter, collector2);

        // Collect all highlighter matches
        Set<String> hlIds = new HashSet<>();
        for (IntervalsQueryMatch match : collector2.getMatches()) {
            addMatch(match);
            hlIds.add(match.getDocId());
        }

        // For any document that didn't have highlighter matches, add back the original
        // matches instead.
        for (IntervalsQueryMatch match : collector.getMatches()) {
            if (!hlIds.contains(match.getDocId()))
                addMatch(match);
        }

    }

    public static final MatcherFactory<IntervalsQueryMatch> FACTORY = new MatcherFactory<IntervalsQueryMatch>() {
        @Override
        public IntervalsMatcher createMatcher(DocumentBatch docs) {
            return new IntervalsMatcher(docs);
        }
    };

}
