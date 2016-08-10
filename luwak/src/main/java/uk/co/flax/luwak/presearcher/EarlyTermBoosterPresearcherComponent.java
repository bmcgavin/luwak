package uk.co.flax.luwak.presearcher;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.payloads.AveragePayloadFunction;
import org.apache.lucene.search.payloads.PayloadScoreQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

/*
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
 * PresearcherComponent that allows you to boost queries by a term postition on
 * each document.
 *
 * Queries are assigned field values by passing them as part of the metadata
 * on a MonitorQuery.
 *
 * N.B. DocumentBatches used with this presearcher component must all have the
 * same value in the filter field, otherwise an IllegalArgumentException will
 * be thrown
 */
public class EarlyTermBoosterPresearcherComponent extends PresearcherComponent {

    private final String field;

    /**
     * Create a new EarlyTermBoosterPresearcherComponent that filters queries on a
     * given field.
     *
     * @param field the field to filter on
     */
    public EarlyTermBoosterPresearcherComponent(String field) {
        this.field = field;
    }


    @Override
    public Query adjustPresearcherQuery(LeafReader reader, Query presearcherQuery) throws IOException {

        Query filterClause = buildFilterClause(reader);
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("ETBP.filterClause : " + filterClause);

        if (filterClause == null) {
            if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("ETBP return pQ");
            return presearcherQuery;
        }

        return filterClause;
        /*
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(presearcherQuery, BooleanClause.Occur.MUST);
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Fucking dumb presearcherQuery : " + presearcherQuery);
        bq.add(filterClause, BooleanClause.Occur.MUST_NOT);
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("ETBP.adjustPresearcherQuery returning : " + bq.build());
        return bq.build();
        */
    }

    private Query buildFilterClause(LeafReader reader) throws IOException {

        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Reader class : " + reader.getClass());
        Terms terms = reader.fields().terms(field);
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Terms hasPositions() : " + terms.hasPositions());
        if (terms == null)
            return null;

        BooleanQuery.Builder bq = new BooleanQuery.Builder();

        int docsInBatch = reader.maxDoc();

        BytesRef term;
        TermsEnum te = terms.iterator();
        if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("ETBP terms size : " + terms.size());
        long boostFactor = terms.size();
        long queryLength = 0;
        TermQuery tq = null;

        //Need to iterate to get length
        while ((term = te.next()) != null) {
            queryLength += term.length;
        }
        te = terms.iterator();
        
        while ((term = te.next()) != null) {
            if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Term offset : " + term.offset);
            if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("Boost factor : " + (float)queryLength / (float)term.offset);
            // we need to check that every document in the batch has the same field values, otherwise
            // this filtering will not work
            if (te.docFreq() != docsInBatch)
                throw new IllegalArgumentException("Some documents in this batch do not have a term value of "
                                                    + field + ":" + Term.toString(term));
            if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("ETBPC term : " +  BytesRef.deepCopyOf(term));
            //tq = new TermQuery(new Term(field, BytesRef.deepCopyOf(term)));
            SpanTermQuery stq = new SpanTermQuery(new Term(field, BytesRef.deepCopyOf(term)));
            SpanFirstQuery sfq = new SpanFirstQuery(stq, 1);
            //tq = new TermQuery(new Term(field, BytesRef.deepCopyOf(term)));
			//tq = new PayloadScoreQuery(stq, new AveragePayloadFunction());
            //sfq.setBoost((float)queryLength / (float)term.offset);
            sfq.setBoost(100.0f);
            //bq.add(tq, BooleanClause.Occur.MUST);
            bq.add(sfq, BooleanClause.Occur.SHOULD);
            if (System.getProperty("luwak.debug", "false").equals("true")) System.out.println("ETBPC bq : " +  bq);
        }

        BooleanQuery built = bq.build();

        if (built.clauses().size() == 0)
            return null;

        return built;
    }

    @Override
    public void adjustQueryDocument(Document doc, Map<String, String> metadata) {
        if (metadata == null || !metadata.containsKey(field))
            return;
        doc.add(new TextField(field, metadata.get(field), Field.Store.YES));
    }

    @Override
    public TokenStream filterDocumentTokens(String field, TokenStream ts) {
        // We don't want tokens from this field to be present in the disjunction,
        // only in the extra filter query.  Otherwise, every doc that matches in
        // this field will be selected!
        if (this.field.equals(field))
            return new EmptyTokenStream();
        return ts;
    }
}

