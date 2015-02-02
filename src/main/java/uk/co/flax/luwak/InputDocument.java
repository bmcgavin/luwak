package uk.co.flax.luwak;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;

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

/**
 * An InputDocument represents a document to be run against registered queries
 * in the Monitor.  It should be constructed using the static #builder() method.
 */
public class InputDocument {

    private static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    }

    public static final String ID_FIELD = "_luwak_id";

    /**
     * Create a new fluent {@link uk.co.flax.luwak.InputDocument.Builder} object.
     * @param id the id
     * @return a Builder
     */
    public static Builder builder(String id) {
        return new Builder(id);
    }

    private final String id;

    // protected constructor - use a Builder to create objects
    protected InputDocument(String id) {
        this.id = id;
    }

    private Document luceneDocument = new Document();

    /**
     * Get the document's ID
     * @return the document's ID
     */
    public String getId() {
        return id;
    }

    public Document getDocument() {
        return luceneDocument;
    }

    /**
     * Fluent interface to construct a new InputDocument
     */
    public static class Builder {

        private final InputDocument doc;

        /**
         * Create a new Builder for an InputDocument with the given id
         * @param id the id of the InputDocument
         */
        public Builder(String id) {
            this.doc = new InputDocument(id);
        }

        /**
         * Add a field to the InputDocument
         *
         * @param field the field name
         * @param text the text content of the field
         *
         * @return the Builder object
         */
        public Builder addField(String field, String text) {
            checkFieldName(field);
            doc.luceneDocument.add(new Field(field, text, FIELD_TYPE));
            return this;
        }

        public Builder addField(IndexableField field) {
            checkFieldName(field.name());
            doc.luceneDocument.add(field);
            return this;
        }

        /**
         * Build the InputDocument
         * @return the InputDocument
         */
        public InputDocument build() {
            doc.luceneDocument.add(new StringField(ID_FIELD, doc.id, Field.Store.YES));
            return doc;
        }

    }

    public static void checkFieldName(String fieldName) {
        if (ID_FIELD.equals(fieldName))
            throw new IllegalArgumentException(ID_FIELD + " is a reserved field name");
    }

}
