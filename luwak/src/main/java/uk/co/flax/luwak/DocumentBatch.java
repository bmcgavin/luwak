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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;

public class DocumentBatch implements Closeable {

    private final Directory directory;
    private final IndexWriter writer;

    private String[] docIds = null;
    private LeafReader reader = null;

    public DocumentBatch(Directory directory, Analyzer analyzer) throws IOException {
        this.directory = directory;
        this.writer = new IndexWriter(directory, new IndexWriterConfig(analyzer));
    }

    public DocumentBatch(Analyzer analyzer) throws IOException {
        this (new RAMDirectory(), analyzer);
    }

    public void addInputDocument(InputDocument doc) throws IOException {
        if (docIds != null)
            throw new IllegalStateException("Cannot add new documents once getIndexReader() has been called");
        this.writer.addDocument(doc.getDocument());
    }

    public synchronized LeafReader getIndexReader() throws IOException {

        if (reader != null)
            return reader;

        writer.commit();
        reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(directory));
        assert reader != null;

        docIds = new String[reader.maxDoc()];
        for (int i = 0; i < docIds.length; i++) {
            docIds[i] = reader.document(i).get(InputDocument.ID_FIELD);     // TODO can this be more efficient?
        }

        return reader;
    }

    public IndexSearcher getSearcher() throws IOException {
        return new IndexSearcher(getIndexReader());
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(writer, directory);
    }

    public String resolveDocId(int docId) {
        return docIds[docId];
    }

    public void addAll(Iterable<InputDocument> inputDocuments) throws IOException {
        for (InputDocument doc : inputDocuments) {
            addInputDocument(doc);
        }
    }

    public int getBatchSize() {
        if (docIds == null)
            throw new IllegalStateException("Cannot call getBatchSize() before getIndexReader()");
        return docIds.length;
    }

    public String[] getDocIds() {
        if (docIds == null)
            throw new IllegalStateException("Cannot call getDocIds() before getIndexReader()");
        return docIds;
    }
}
