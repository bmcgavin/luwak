package uk.co.flax.luwak;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;

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
 * Class used to match candidate queries selected by a Presearcher from a Monitor
 * query index.
 */
public abstract class CandidateMatcher<T extends QueryMatch> {

    protected final DocumentBatch docs;
    protected long slowLogLimit;

    private final List<MatchError> errors = new LinkedList<>();
    private final Map<String, MatchHolder<T>> matches = new HashMap<>();

    private long queryBuildTime = -1;
    private long searchTime = System.nanoTime();
    private int queriesRun = -1;

    protected final StringBuilder slowlog = new StringBuilder();

    private static class MatchHolder<T> {
        Map<String, T> matches = new HashMap<>();
    }

    /**
     * Creates a new CandidateMatcher for the supplied InputDocument
     * @param docs the documents to run queries against
     */
    public CandidateMatcher(DocumentBatch docs) {
        this.docs = docs;
    }

    /**
     * Runs the supplied query against this CandidateMatcher's InputDocument, storing any
     * resulting match.
     *
     * @param queryId the query id
     * @param matchQuery the query to run
     * @param highlightQuery an optional query to use for highlighting.  May be null
     * @throws IOException
     * @return true if the query matches
     */
    public abstract void matchQuery(String queryId, Query matchQuery, Query highlightQuery) throws IOException;

    protected void addMatch(String queryId, int docId, T match) {
        addMatch(queryId, docs.resolveDocId(docId), match);
    }

    private void addMatch(String queryId, String docId, T match) {
        MatchHolder<T> docMatches = matches.get(docId);
        if (docMatches == null) {
            docMatches = new MatchHolder<>();
            matches.put(docId, docMatches);
        }
        if (docMatches.matches.containsKey(queryId)) {
            docMatches.matches.put(queryId, resolve(match, docMatches.matches.get(queryId)));
        }
        else {
            docMatches.matches.put(queryId, match);
        }
    }

    protected void addMatch(T match) {
        addMatch(match.getQueryId(), match.getDocId(), match);
    }

    /**
     * If two matches from the same query are found (for example, two branches of a disjunction),
     * combine them.
     * @param match1 the first match found
     * @param match2 the second match found
     * @return a Match object that combines the two
     */
    public abstract T resolve(T match1, T match2);

    /**
     * Called by the Monitor if running a query throws an Exception
     * @param e the MatchError detailing the problem
     */
    public void reportError(MatchError e) {
        this.errors.add(e);
    }

    public void finish(long buildTime, int queryCount) {
        this.queryBuildTime = buildTime;
        this.queriesRun = queryCount;
        this.searchTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - searchTime, TimeUnit.NANOSECONDS);
    }

    /*
     * Called by the Monitor
     */
    public void setSlowLogLimit(long t) {
        this.slowLogLimit = t;
    }

    /**
     * Returns the QueryMatch for the given query, or null if it did not match
     * @param queryId the query id
     */
    protected T matches(String docId, String queryId) {
        MatchHolder<T> docMatches = matches.get(docId);
        if (docMatches == null)
            return null;
        return docMatches.matches.get(queryId);
    }

    public Matches<T> getMatches() {
        Map<String, DocumentMatches<T>> results = new HashMap<>();
        for (Map.Entry<String, MatchHolder<T>> entry : matches.entrySet()) {
            DocumentMatches<T> docMatches = new DocumentMatches<>(entry.getKey(), entry.getValue().matches.values());
            results.put(entry.getKey(), docMatches);
        }
        return new Matches<>(results, errors, queryBuildTime, searchTime, queriesRun, docs.getBatchSize(), slowlog.toString());
    }

    public LeafReader getIndexReader() throws IOException {
        return docs.getIndexReader();
    }
}
