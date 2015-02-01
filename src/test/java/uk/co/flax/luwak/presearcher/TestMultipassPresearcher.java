package uk.co.flax.luwak.presearcher;

import java.io.IOException;

import org.junit.Test;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;

import static uk.co.flax.luwak.util.MatchesAssert.assertThat;

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

public class TestMultipassPresearcher extends PresearcherTestBase {

    @Override
    protected Presearcher createPresearcher() {
        return new MultipassTermFilteredPresearcher(4, 0.0f);
    }

    @Test
    public void testSimpleBoolean() throws IOException {

        monitor.update(new MonitorQuery("1", "field:\"hello world\""),
                       new MonitorQuery("2", "field:world"),
                       new MonitorQuery("3", "field:\"hello there world\""),
                       new MonitorQuery("4", "field:\"this and that\""));

        Matches<QueryMatch> matches = monitor.match(buildDoc("doc1", "field", "hello world and goodbye"),
                                                        SimpleMatcher.FACTORY);
        assertThat(matches)
                .hasQueriesRunCount(2)
                .matchesQuery("1", "doc1");

    }

    @Test
    public void testComplexBoolean() throws IOException {

        monitor.update(new MonitorQuery("1", "field:(+foo +bar +(badger cormorant))"));

        assertThat(monitor.match(buildDoc("doc1", "field", "a badger walked into a bar"), SimpleMatcher.FACTORY))
                .hasMatchCount(0)
                .hasQueriesRunCount(0);

        assertThat(monitor.match(buildDoc("doc2", "field", "foo badger cormorant"), SimpleMatcher.FACTORY))
                .hasMatchCount(0)
                .hasQueriesRunCount(0);

        assertThat(monitor.match(buildDoc("doc3", "field", "bar badger foo"), SimpleMatcher.FACTORY))
                .hasMatchCount(1);

    }
}
