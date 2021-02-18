/*
 * DirectoryCheck.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Closes the directory after commit.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class DirectoryCommitCheckAsync implements FDBRecordContext.CommitCheckAsync {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexMaintainer.class);
    private final Directory directory;

    /**
     * Creates a lucene directory from a subspace (keyspace) and a transaction.
     *
     * @param subspace subspace
     * @param txn txn
     */
    public DirectoryCommitCheckAsync(@Nonnull Subspace subspace, @Nonnull Transaction txn) {
        this.directory = new FDBDirectory(subspace, txn);
    }

    /**
     * Close Lucene Directory while swallowing errors.
     *
     * @return CompletableFuture
     */
    @Nonnull
    @Override
    public CompletableFuture<Void> checkAsync() {
        LOGGER.trace("closing directory check");
        return CompletableFuture.runAsync(() -> IOUtils.closeWhileHandlingException(directory));
    }

    @Nonnull
    public Directory getDirectory() {
        return directory;
    }

    /**
     * Attempts to get the commit check from the context and if it cannot find it, creates one and adds it to the context.
     *
     * @param state state
     * @return DirectoryCommitCheckAsync
     */
    @Nonnull
    protected static DirectoryCommitCheckAsync getOrCreateDirectoryCommitCheckAsync(@Nonnull final IndexMaintainerState state) {
        synchronized (state) {
            DirectoryCommitCheckAsync directoryCheck = state.context.getCommitCheck(getDirectoryName(state), DirectoryCommitCheckAsync.class);
            if (directoryCheck == null) {
                directoryCheck = new DirectoryCommitCheckAsync(state.indexSubspace, state.transaction);
                state.context.addCommitCheck(getDirectoryName(state), directoryCheck);
            }
            return directoryCheck;
        }
    }

    /**
     * The directory name in the context.
     *
     * @param state state
     * @return String
     */
    private static String getDirectoryName(@Nonnull final IndexMaintainerState state) {
        return "directory$" + state.index.getName();
    }
}

