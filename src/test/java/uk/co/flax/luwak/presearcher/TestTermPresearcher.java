package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;

import static uk.co.flax.luwak.util.MatchesAssert.assertThat;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
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
public class TestTermPresearcher extends PresearcherTestBase {

    @Test
    public void filtersOnTermQueries() throws IOException {

        MonitorQuery query1
                = new MonitorQuery("1", "furble");
        MonitorQuery query2
                = new MonitorQuery("2", "document");
        monitor.update(query1, query2);

        Matches<QueryMatch> matcher = monitor.match(buildDoc("doc1", TEXTFIELD, "this is a test document"), SimpleMatcher.FACTORY);
        assertThat(matcher)
                .hasMatchCount("doc1", 1)
                .hasQueriesRunCount(1);

    }

    @Test
    public void ignoresTermsOnNotQueries() throws IOException {

        monitor.update(new MonitorQuery("1", "document -test"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "this is a test document"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc1", 0)
                .hasQueriesRunCount(1);

        assertThat(monitor.match(buildDoc("doc2", TEXTFIELD, "weeble sclup test"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc2", 0)
                .hasQueriesRunCount(0);
    }

    @Test
    public void matchesAnyQueries() throws IOException {

        monitor.update(new MonitorQuery("1", "/hell./"));

        assertThat(monitor.match(buildDoc("doc1", TEXTFIELD, "hello"), SimpleMatcher.FACTORY))
                .hasMatchCount("doc1", 1)
                .hasQueriesRunCount(1);

    }

    @Override
    protected Presearcher createPresearcher() {
        return new TermFilteredPresearcher();
    }

    @Test
    public void testAnyTermsAreCorrectlyAnalyzed() {

        TermFilteredPresearcher presearcher = new TermFilteredPresearcher();
        QueryTree qt = presearcher.extractor.buildTree(new MatchAllDocsQuery());

        Map<String, StringBuilder> extractedTerms = presearcher.collectTerms(qt);

        Assertions.assertThat(extractedTerms.size()).isEqualTo(1);

    }
}
