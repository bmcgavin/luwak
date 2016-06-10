package uk.co.flax.luwak;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;

/**
 * A collection of InputDocuments to be matched.
 *
 * A batch containing a single InputDocument uses a lucene MemoryIndex for indexing,
 * otherwise a RAMDirectory is used to hold the documents.
 *
 * To build a batch, either use one of the static factory methods, or a Builder object:
 * <pre>
 *     DocumentBatch batch1 = DocumentBatch.of(doc1, doc2)
 *     DocumentBatch batch2 = new DocumentBatch.Builder()
 *                                  .setSimilarity(new MySimilarity())
 *                                  .add(doc1)
 *                                  .addAll(listOfDocs)
 *                                  .build()
 * </pre>
 */
public abstract class DocumentBatch implements Closeable, Iterable<InputDocument> {

    /** The {@link Similarity} to be used for scoring (if scoring is required) */
    protected final Similarity similarity;

    /** A list of {@link InputDocument} objects to match */
    protected final List<InputDocument> documents = new ArrayList<>();

    /**
     * Create a DocumentBatch containing a single InputDocument
     */
    public static DocumentBatch of(InputDocument doc, Similarity sim) throws IOException {
        return new DocumentBatch.Builder().add(doc).setSimilarity(sim).build();
    }
 
    /**
     * Create a DocumentBatch containing a single InputDocument
     */
    public static DocumentBatch of(InputDocument doc) throws IOException {
        return new DocumentBatch.Builder().add(doc).build();
    }

    /**
     * Create a DocumentBatch containing a set of InputDocuments
     */
    public static DocumentBatch of(Collection<InputDocument> docs) {
        return new DocumentBatch.Builder().addAll(docs).build();
    }

    /**
     * Create a DocumentBatch containing a set of InputDocuments
     */
    public static DocumentBatch of(Collection<InputDocument> docs, Similarity sim) {
        return new DocumentBatch.Builder().addAll(docs).setSimilarity(sim).build();
    }

    /**
     * Create a DocumentBatch containing a set of InputDocuments
     */
    public static DocumentBatch of(InputDocument... docs) throws IOException {
        return of(Arrays.asList(docs));
    }

    /**
     * Create a DocumentBatch containing a set of InputDocuments
     */
    public static DocumentBatch of(Similarity sim, InputDocument... docs) throws IOException {
        return of(Arrays.asList(docs), sim);
    }
    
    /**
     * Builder class for DocumentBatch
     */
    public static class Builder {

        private Similarity similarity = new DefaultSimilarity();
        private List<InputDocument> documents = new ArrayList<>();

        /** Add an InputDocument */
        public Builder add(InputDocument doc) {
            documents.add(doc);
            return this;
        }

        /** Add a collection of InputDocuments */
        public Builder addAll(Collection<InputDocument> docs) {
            documents.addAll(docs);
            return this;
        }

        /** Set the {@link Similarity} to be used for scoring this batch */
        public Builder setSimilarity(Similarity similarity) {
            this.similarity = similarity;
            return this;
        }

        /** Create the DocumentBatch */
        public DocumentBatch build() {
            if (documents.size() == 0)
                throw new IllegalStateException("Cannot build DocumentBatch with zero documents");
            if (documents.size() == 1)
                return new SingletonDocumentBatch(documents, similarity);
            return new MultiDocumentBatch(documents, similarity);
        }

    }

    /**
     * Create a new DocumentBatch
     * @param documents the documents to match
     * @param similarity the {@link Similarity} to use for scoring
     */
    protected DocumentBatch(Collection<InputDocument> documents, Similarity similarity) {
        this.similarity = similarity;
        this.documents.addAll(documents);
    }

    /**
     * @return a {@link LeafReader} over the documents in this batch
     * @throws IOException on error
     */
    public abstract LeafReader getIndexReader() throws IOException;

    /**
     * Convert the lucene docid for a document in the batch to the luwak docid
     * @param docId the lucene docid
     * @return the luwak docid
     */
    public abstract String resolveDocId(int docId);

    /**
     * @return an {@link IndexSearcher} over the documents in this batch
     * @throws IOException on error
     */
    public IndexSearcher getSearcher() throws IOException {
        IndexSearcher searcher = new IndexSearcher(getIndexReader());
        searcher.setSimilarity(similarity);
        return searcher;
    }

    @Override
    public Iterator<InputDocument> iterator() {
        return documents.iterator();
    }

    /**
     * @return the number of documents in the batch
     */
    public int getBatchSize() {
        return documents.size();
    }

    // Implementation of DocumentBatch for collections of documents
    private static class MultiDocumentBatch extends DocumentBatch {

        private final Directory directory = new RAMDirectory();
        private LeafReader reader = null;
        private String[] docIds = null;

        MultiDocumentBatch(List<InputDocument> docs, Similarity similarity) {
            super(docs, similarity);
            System.out.println("MutiDocumentBatch");
            assert docs.size() > 1;
            IndexWriterConfig iwc = new IndexWriterConfig(docs.get(0).getAnalyzers()).setSimilarity(similarity);
            try (IndexWriter writer = new IndexWriter(directory, iwc)) {
                this.reader = build(writer);
            }
            catch (IOException e) {
                throw new RuntimeException(e);  // This is a RAMDirectory, so should never happen...
            }
        }

        @Override
        public LeafReader getIndexReader() throws IOException {
            return reader;
        }

        private LeafReader build(IndexWriter writer) throws IOException {

            for (InputDocument doc : documents) {
                writer.addDocument(doc.getDocument());
            }

            writer.commit();
            LeafReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(directory));
            assert reader != null;

            docIds = new String[reader.maxDoc()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = reader.document(i).get(InputDocument.ID_FIELD);     // TODO can this be more efficient?
            }

            return reader;

        }

        @Override
        public String resolveDocId(int docId) {
            return docIds[docId];
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(reader, directory);
        }

    }

    // Specialized class for batches containing a single object - MemoryIndex benchmarks as
    // better performing than RAMDirectory for this case
    private static class SingletonDocumentBatch extends DocumentBatch {

        private final MemoryIndex memoryindex = new MemoryIndex(true, true);
        private final LeafReader reader;

        private SingletonDocumentBatch(Collection<InputDocument> documents, Similarity similarity) {
            super(documents, similarity);
            System.out.println("SingletonDocumentBatch");
            assert documents.size() == 1;
            memoryindex.setSimilarity(similarity);
            try {
                for (InputDocument doc : documents) {
                    for (IndexableField field : doc.getDocument()) {
                        System.out.println("Adding field : " + field.name() + ":" + doc);

                        PerFieldAnalyzerWrapper analyzers = doc.getAnalyzers();
                        System.out.println("Analyzers : " + analyzers);
                        TokenStream ts = field.tokenStream(doc.getAnalyzers(), null);
                        /*
                        if (field.name() == "text") {
                            OffsetAttribute oa = ts.addAttribute(OffsetAttribute.class);
                            BoostAttribute ba = ts.addAttribute(BoostAttribute.class);

                            float boost = 10.0f;
                            ts.reset();
                            while (ts.incrementToken()) {
                                System.out.println("Field content : " + ts.reflectAsString(true));
                                System.out.println("Field offset : " + oa.startOffset());
                                System.out.println("Field boost : " + ba.getBoost());
                                ba.setBoost(boost);
                                if (boost >= 2.0f) {
                                    boost -= 1.0f;
                                }
                                System.out.println("Field boost : " + ba.getBoost());
                            }
                            ts.end();
                            ts.close();

                            ts.reset();
                            ba = ts.addAttribute(BoostAttribute.class);
                            while (ts.incrementToken()) {
                                System.out.println("Field content : " + ts.reflectAsString(true));
                                System.out.println("Field boost : " + ba.getBoost());
                            }
                            ts.end();
                            ts.close();
                            ts.reset();
                        }
                        */

                        
                        memoryindex.addField(field.name(), ts);
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            memoryindex.freeze();
            reader = (LeafReader) memoryindex.createSearcher().getIndexReader();
        }

        @Override
        public LeafReader getIndexReader() throws IOException {
            return reader;
        }

        @Override
        public String resolveDocId(int docId) {
            assert docId == 0;
            return documents.get(0).getId();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

}
