package uk.co.flax.luwak.intervals;

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

import java.io.IOException;
import java.util.*;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalCollector;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.util.FixedBitSet;
import uk.co.flax.luwak.DocumentBatch;

/**
 * a specialized Collector that uses an {@link IntervalIterator} to collect
 * match positions from a Scorer.
 */
public class QueryIntervalsMatchCollector extends SimpleCollector {

    private IntervalIterator positions;

    private final String queryId;
    private final DocumentBatch docs;
    private final FixedBitSet matchBits;

    private final List<IntervalsQueryMatch> matches = new LinkedList<>();

    public QueryIntervalsMatchCollector(String queryId, DocumentBatch docs) throws IOException {
        this.queryId = queryId;
        this.docs = docs;
        this.matchBits = new FixedBitSet(docs.getIndexReader().maxDoc());
    }

    public Collection<IntervalsQueryMatch> getMatches() {
        return matches;
    }

    public FixedBitSet getMatchBitset() {
        return matchBits;
    }

    @Override
    public void collect(int doc) throws IOException {
        String docId = docs.resolveDocId(doc);
        MatchBuilder builder = new MatchBuilder(queryId, docId);
        if (positions != null) {
            positions.scorerAdvanced(doc);
            while(positions.next() != null) {
                positions.collect(builder);
            }
        }
        else {
            builder.setMatch();
        }
        matches.add(builder.build());
        matchBits.set(doc);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        try {
            positions = scorer.intervals(true);
        }
        catch (UnsupportedOperationException e) {
            // Query doesn't support positions, so we just say if it's a match or not
        }
    }

    @Override
    public Weight.PostingFeatures postingFeatures() {
        return Weight.PostingFeatures.OFFSETS;
    }


    private static class MatchBuilder implements IntervalCollector {

        private final Map<String, List<IntervalsQueryMatch.Hit>> hits = new HashMap<>();

        private boolean match;

        private String queryId;

        private String docId;

        public MatchBuilder(String queryId, String docId) {
            this.queryId = queryId;
            this.docId = docId;
        }

        public void setMatch() {
            this.match = true;
        }

        public void addInterval(Interval interval) {
            if (!hits.containsKey(interval.field))
                hits.put(interval.field, new ArrayList<IntervalsQueryMatch.Hit>());
            hits.get(interval.field)
                    .add(new IntervalsQueryMatch.Hit(interval.begin, interval.offsetBegin, interval.end, interval.offsetEnd));
        }

        public IntervalsQueryMatch build() {
            if (!match && hits.size() == 0)
                return null;
            return new IntervalsQueryMatch(queryId, docId, hits);
        }

        @Override
        public void collectLeafPosition(Scorer scorer, Interval interval, int docID) {
            addInterval(interval);
        }

        @Override
        public void collectComposite(Scorer scorer, Interval interval,
                                     int docID) {
            //offsets.add(new Offset(interval.begin, interval.end, interval.offsetBegin, interval.offsetEnd));
        }

    }

}
