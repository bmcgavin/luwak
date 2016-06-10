package uk.co.flax.luwak.presearcher;

/*
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

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefIterator;
import uk.co.flax.luwak.Presearcher;
import uk.co.flax.luwak.QueryTermFilter;
import uk.co.flax.luwak.analysis.BytesRefFilteredTokenFilter;
import uk.co.flax.luwak.analysis.TermsEnumTokenStream;
import uk.co.flax.luwak.termextractor.QueryAnalyzer;
import uk.co.flax.luwak.termextractor.QueryTerm;
import uk.co.flax.luwak.termextractor.querytree.QueryTree;
import uk.co.flax.luwak.termextractor.querytree.QueryTreeViewer;
import uk.co.flax.luwak.termextractor.querytree.TreeWeightor;

/**
 * Presearcher implementation that uses terms extracted from queries to index
 * them in the Monitor, and builds a BooleanQuery from InputDocuments to match
 * them.
 *
 * This Presearcher uses a QueryTermExtractor to extract terms from queries.
 */
public class TermFilteredPresearcher extends Presearcher {

    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    protected final QueryAnalyzer extractor;

    private final List<PresearcherComponent> components = new ArrayList<>();

    public static final String ANYTOKEN_FIELD = "__anytokenfield";

    public static final String ANYTOKEN = "__ANYTOKEN__";

    public TermFilteredPresearcher(TreeWeightor weightor, PresearcherComponent... components) {
        this.extractor = QueryAnalyzer.fromComponents(weightor, components);
        this.components.addAll(Arrays.asList(components));
    }

    public TermFilteredPresearcher(PresearcherComponent... components) {
        this(TreeWeightor.DEFAULT_WEIGHTOR, components);
    }

    public TermFilteredPresearcher() {
        this(TreeWeightor.DEFAULT_WEIGHTOR);
    }

    @Override
    public final Query buildQuery(LeafReader reader, QueryTermFilter queryTermFilter) {
        try {
            DocumentQueryBuilder queryBuilder = getQueryBuilder();
            System.out.println("DocumentQueryBuilder : " + queryBuilder);
            System.out.println("QueryTermFilter : " + queryTermFilter);
            for (String field : reader.fields()) {

                System.out.println("Adding field " + field);
                TokenStream ts = new TermsEnumTokenStream(reader.terms(field).iterator());
                System.out.println("tokenstream (new) : " + ts);
                for (PresearcherComponent component : components) {
                    ts = component.filterDocumentTokens(field, ts);
                }
                System.out.println("tokenstream (postloop) : " + ts);

                System.out.println("qTF.gT : " + queryTermFilter.getTerms(field));
                
                ts = new BytesRefFilteredTokenFilter(ts, queryTermFilter.getTerms(field));
                System.out.println("tokenstream (after brftf) : " + ts);

                TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
                System.out.println("tokenstream (after addattribute) : " + ts);
                while (ts.incrementToken()) {
                    queryBuilder.addTerm(field, BytesRef.deepCopyOf(termAtt.getBytesRef()));
                }
                System.out.println("Out of while");

            }
            Query presearcherQuery = queryBuilder.build();

            System.out.println("presearcherQuery (prebuild) : " + presearcherQuery);
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.add(presearcherQuery, BooleanClause.Occur.SHOULD);
            bq.add(new TermQuery(new Term(ANYTOKEN_FIELD, ANYTOKEN)), BooleanClause.Occur.SHOULD);
            presearcherQuery = bq.build();
            System.out.println("presearcherQuery (build) : " + presearcherQuery);

            for (PresearcherComponent component : components) {
                presearcherQuery = component.adjustPresearcherQuery(reader, presearcherQuery);
            }
            System.out.println("presearcherQuery (adjust) : " + presearcherQuery);

            return presearcherQuery;
        }
        catch (IOException e) {
            // We're a MemoryIndex, so this shouldn't happen...
            throw new RuntimeException(e);
        }
    }

    protected BytesRefHash buildTermsHash(String field, LeafReader reader) throws IOException {
        BytesRefHash terms = new BytesRefHash();
        Terms t = reader.terms(field);
        if (t == null) {
            return terms;
        }
        TermsEnum te = t.iterator();
        BytesRef term;
        while ((term = te.next()) != null) {
            System.out.println("Adding term : " + term);
            terms.add(term);
        }
        return terms;
    }

    protected DocumentQueryBuilder getQueryBuilder() {
        return new DocumentQueryBuilder() {

            List<Term> terms = new ArrayList<>();

            @Override
            public void addTerm(String field, BytesRef term) throws IOException {
                System.out.println("Adding term : " + field + ":" + term);
                terms.add(new Term(field, term));
            }

            @Override
            public Query build() {
                System.out.println("Building termsQuery : " + terms);
                return new TermsQuery(terms);
            }
        };
    }

    public static final FieldType QUERYFIELDTYPE;
    static {
        QUERYFIELDTYPE = new FieldType(TextField.TYPE_NOT_STORED);
        QUERYFIELDTYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        QUERYFIELDTYPE.freeze();
    }

    @Override
    public final Document indexQuery(Query query, Map<String, String> metadata) {

        QueryTree querytree = extractor.buildTree(query);
        //DEBUG System.out.println("indexQuery outputting query tree");
        //DEBUG showQueryTree(query, System.out);
        Document doc = buildQueryDocument(querytree);

        for (PresearcherComponent component : components) {
            component.adjustQueryDocument(doc, metadata);
        }

        return doc;
    }

    /**
     * Debugging: write the parsed query tree to a PrintStream
     *
     * @param query the query to analyze
     * @param out   a {@link PrintStream}
     */
    public void showQueryTree(Query query, PrintStream out) {
        QueryTreeViewer.view(extractor.buildTree(query), extractor.weightor, out);
    }

    protected Document buildQueryDocument(QueryTree querytree) {
        Map<String, BytesRefHash> fieldTerms = collectTerms(querytree);
        Document doc = new Document();
        for (Map.Entry<String, BytesRefHash> entry : fieldTerms.entrySet()) {
            //DEBUG System.out.println("Adding new field to : " + entry.getKey() + ":" + entry.getValue());
            doc.add(new Field(entry.getKey(),
                    new TermsEnumTokenStream(new BytesRefHashIterator(entry.getValue())), QUERYFIELDTYPE));
        }
        System.out.println("TFP.buildQueryDocument : " + doc);
        return doc;
    }

    protected class BytesRefHashIterator implements BytesRefIterator {

        final BytesRef scratch = new BytesRef();
        final BytesRefHash terms;
        final int[] sortedTerms;
        int upto = -1;


        public BytesRefHashIterator(BytesRefHash terms) {
            this.terms = terms;
            this.sortedTerms = terms.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
        }

        @Override
        public BytesRef next() throws IOException {
            if (upto >= sortedTerms.length)
                return null;
            upto++;
            if (sortedTerms[upto] == -1)
                return null;
            this.terms.get(sortedTerms[upto], scratch);
            return scratch;
        }
    }

    protected Map<String, BytesRefHash> collectTerms(QueryTree tree) {

        Map<String, BytesRefHash> fieldTerms = new HashMap<>();
        //DEBUG System.out.println("TFP in collectTerms : " + tree);

        for (QueryTerm queryTerm : extractor.collectTerms(tree)) {
            if (queryTerm.type.equals(QueryTerm.Type.ANY)) {
                if (!fieldTerms.containsKey(ANYTOKEN_FIELD)) {
                    BytesRefHash hash = new BytesRefHash();
                    hash.add(new BytesRef(ANYTOKEN));
                    fieldTerms.put(ANYTOKEN_FIELD, hash);
                }
            }
            else {
                if (!fieldTerms.containsKey(queryTerm.term.field()))
                    fieldTerms.put(queryTerm.term.field(), new BytesRefHash());

                BytesRefHash termslist = fieldTerms.get(queryTerm.term.field());
                if (queryTerm.type.equals(QueryTerm.Type.EXACT)) {
                    termslist.add(queryTerm.term.bytes());
                } else {
                    termslist.add(queryTerm.term.bytes());
                    for (PresearcherComponent component : components) {
                        BytesRef extratoken = component.extraToken(queryTerm);
                        if (extratoken != null)
                            termslist.add(extratoken);
                    }
                }
            }
        }
        System.out.println("TFP.collectTerms : " + fieldTerms);
        return fieldTerms;
    }


}
