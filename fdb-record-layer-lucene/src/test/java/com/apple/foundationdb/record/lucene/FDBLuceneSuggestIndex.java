/*
 * FDBLuceneSuggestIndex.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Set;

/**
 * Index to validate suggestion logic works on the directory implementation.
 *
 */
public class FDBLuceneSuggestIndex {
    private final Directory directory;
    private final Analyzer analyzer;
    private final AnalyzingInfixSuggester suggester;

    public FDBLuceneSuggestIndex(Directory directory, Analyzer analyzer) throws IOException {
        this.directory = directory;
        this.analyzer = analyzer;
        this.suggester = new AnalyzingInfixSuggester(directory, analyzer);
    }


    public void add(BytesRef text, Set<BytesRef> contexts, long weight, BytesRef payload) throws IOException {
        suggester.add(text, contexts, weight, payload);
    }

    public void refresh() throws IOException {
        suggester.refresh();
    }

}
