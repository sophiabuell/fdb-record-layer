/*
 * KeySpaceDirectoryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.TestHelpers.assertThrows;
import static com.apple.foundationdb.record.TestHelpers.eventually;
import static com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.DEFAULT_CHECK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeySpaceDirectory}.
 */
@Tag(Tags.RequiresFDB)
public class KeySpaceDirectoryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeySpaceDirectoryTest.class);

    private static class KeyTypeValue {
        KeyType keyType;
        @Nullable
        Object value;
        @Nullable
        Object value2;
        Supplier<Object> generator;

        public KeyTypeValue(KeyType keyType, @Nullable Object value, @Nullable Object value2, Supplier<Object> generator) {
            this.keyType = keyType;
            this.value = value;
            this.value2 = value2;
            this.generator = generator;
            assertTrue(keyType.isMatch(value));
            assertTrue(keyType.isMatch(generator.get()));
        }
    }

    private Random random = new Random();

    private final List<KeyTypeValue> valueOfEveryType = new ImmutableList.Builder<KeyTypeValue>()
            .add(new KeyTypeValue(KeyType.NULL, null, null, () -> null))
            .add(new KeyTypeValue(KeyType.BYTES, new byte[] { 0x01, 0x02 }, new byte[] { 0x03, 0x04 }, () -> {
                int size = random.nextInt(10) + 1;
                byte[] bytes = new byte[size];
                random.nextBytes(bytes);
                return bytes;
            }))
            .add(new KeyTypeValue(KeyType.STRING, "hello", "goodbye", () -> RandomStringUtils.random(random.nextInt(10) + 1)))
            .add(new KeyTypeValue(KeyType.LONG, 11L,  -11L, () -> random.nextLong()))
            .add(new KeyTypeValue(KeyType.FLOAT, 3.2f, -5.4f, () -> random.nextFloat()))
            .add(new KeyTypeValue(KeyType.DOUBLE, 9.7d, -3845.6d, () -> random.nextDouble()))
            .add(new KeyTypeValue(KeyType.BOOLEAN, true, false, () -> random.nextBoolean()))
            .add(new KeyTypeValue(KeyType.UUID, UUID.randomUUID(), UUID.randomUUID(), () -> UUID.randomUUID()))
            .build();

    // Catch if someone adds a new type to make sure that we account for it in this test harness
    @Test
    public void testValueOfEveryTypeReallyIsEveryType() {
        List<KeyType> keyTypes = Lists.newArrayList(KeyType.values());
        Iterator<KeyType> iter = keyTypes.iterator();
        while (iter.hasNext()) {
            KeyType keyType = iter.next();
            for (KeyTypeValue value : valueOfEveryType) {
                if (value.keyType == keyType) {
                    iter.remove();
                    break;
                }
            }
        }
        assertTrue(keyTypes.isEmpty(), "A new type has been added that is not being tested: " + keyTypes);
    }

    @Test
    public void testRestrictSubdirDuplicateName() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root1", KeyType.STRING),
                        new KeySpaceDirectory("root1", KeyType.LONG)));
    }

    @Test
    public void testRestrictSubdirDuplicateType() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root1", KeyType.STRING),
                        new KeySpaceDirectory("root2", KeyType.STRING)));
    }

    @Test
    public void testAllowDifferentConstantValueOfSameType() {
        new KeySpace(
                new KeySpaceDirectory("root1",  KeyType.STRING, "production"),
                new KeySpaceDirectory("root2",  KeyType.STRING, "test"));
    }

    @Test
    public void testRestrictSameConstantValueOfSameType() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root1", KeyType.STRING, "production"),
                        new KeySpaceDirectory("root1", KeyType.STRING, "production")));
    }

    @Test
    public void testRestrictAnyLongAndDirectoryLayerLong() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1"))
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG))));

        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1",  "A"))
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG))));
    }

    @Test
    public void testRestrictConstantLongAndDirectoryLayerLong() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1"))
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG, 10L))));

        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1", "A"))
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG, 10L))));
    }

    @Test
    public void testRestrictAnyDirectoryLayerAndConstantDirectoryLayer() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1", "A"))
                                .addSubdirectory(new DirectoryLayerDirectory("dir2"))));
    }

    @Test
    public void testBadDirectoryLayerTypes() throws Exception {
        for (KeyType keyType : KeyType.values()) {
            for (KeyTypeValue keyTypeValue : valueOfEveryType) {
                if (! (keyTypeValue.keyType == KeyType.STRING)) {
                    assertThrows(RecordCoreArgumentException.class, () ->
                            new KeySpace(new DirectoryLayerDirectory("root", keyTypeValue.value)));
                } else {
                    new KeySpace(new DirectoryLayerDirectory("root", keyTypeValue.value));
                }
            }
        }
    }

    @Test
    public void testRestrictSameConstantDirectoryLayer() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root", KeyType.STRING, "production")
                                .addSubdirectory(new DirectoryLayerDirectory("dir1", "A"))
                                .addSubdirectory(new DirectoryLayerDirectory("dir2", "A"))));
    }

    @Test
    public void testAllowDifferentConstantDirectoryLayer() throws Exception {
        new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "production")
                        .addSubdirectory(new DirectoryLayerDirectory("dir1", "A"))
                        .addSubdirectory(new DirectoryLayerDirectory("dir2", "B")));
    }

    @Test
    public void testRestrictConstantAndAnyValueOfSameType() throws Exception {
        assertThrows(RecordCoreArgumentException.class, () ->
                new KeySpace(
                        new KeySpaceDirectory("root1", KeyType.LONG, 1L),
                        new KeySpaceDirectory("root2", KeyType.LONG)));
    }

    @Test
    public void testBadConstantForType() throws Exception {
        for (KeyType keyType : KeyType.values()) {
            for (KeyTypeValue keyTypeValue : valueOfEveryType) {
                if (! (keyType == keyTypeValue.keyType)) {
                    assertThrows(RecordCoreArgumentException.class, () ->
                            new KeySpace(new KeySpaceDirectory("root", keyType, keyTypeValue.value)));
                } else {
                    new KeySpace(new KeySpaceDirectory("root", keyType, keyTypeValue.value));
                }
            }
        }
    }

    @Test
    public void testPathToAndFromTuple() throws Exception {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("production", "production")
                        .addSubdirectory(new KeySpaceDirectory("userid", KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("application")
                                                .addSubdirectory(new KeySpaceDirectory("dataStore", KeyType.NULL))
                                                .addSubdirectory(new DirectoryLayerDirectory("metadataStore", "S")))),
                new DirectoryLayerDirectory("test", "test")
                        .addSubdirectory(new KeySpaceDirectory("userid", KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("application")
                                        .addSubdirectory(new KeySpaceDirectory("dataStore", KeyType.NULL))
                                        .addSubdirectory(new DirectoryLayerDirectory("metadataStore", "S")))));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        final Tuple path1Tuple;
        final Tuple path2Tuple;
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath path1 = root.path(context, "production")
                    .add("userid", 123456789L)
                    .add("application", "com.mybiz.application1")
                    .add("dataStore");
            path1Tuple = path1.toTuple();

            KeySpacePath path2 = root.path(context, "test")
                    .add("userid", 987654321L)
                    .add("application", "com.mybiz.application2")
                    .add("metadataStore");
            path2Tuple = path2.toTuple();
            context.commit();
        }

        final Tuple path1ExpectedTuple;
        final Tuple path2ExpectedTuple;
        try (FDBRecordContext context = database.openContext()) {
            List<Long> entries = resolveBatch(context, "production", "test",
                    "com.mybiz.application1", "com.mybiz.application2", "S");
            path1ExpectedTuple = Tuple.from(entries.get(0), 123456789L, entries.get(2), null);
            path2ExpectedTuple = Tuple.from(entries.get(1), 987654321L, entries.get(3), entries.get(4));

            assertEquals(path1ExpectedTuple, path1Tuple);
            assertEquals(path2ExpectedTuple, path2Tuple);

            // Now, make sure that we can take a tuple and turn it back into a keyspace path.
            List<KeySpacePath> path1 = root.pathFromKey(context, path1ExpectedTuple).flatten();
            assertEquals("production", path1.get(0).getDirectoryName());
            assertEquals(entries.get(0), path1.get(0).getResolvedValue().get());
            assertEquals("userid", path1.get(1).getDirectoryName());
            assertEquals(123456789L, path1.get(1).getResolvedValue().get());
            assertEquals("application", path1.get(2).getDirectoryName());
            assertEquals(entries.get(2), path1.get(2).getResolvedValue().get());
            assertEquals("dataStore", path1.get(3).getDirectoryName());
            assertEquals(null, path1.get(3).getResolvedValue().get());

            // Tack on extra value to make sure it is in the remainder.
            Tuple extendedPath2 = path2ExpectedTuple.add(10L);
            List<KeySpacePath> path2 = root.pathFromKey(context, extendedPath2).flatten();
            assertEquals("test", path2.get(0).getDirectoryName());
            assertEquals("userid", path2.get(1).getDirectoryName());
            assertEquals("application", path2.get(2).getDirectoryName());
            assertEquals("metadataStore", path2.get(3).getDirectoryName());
            assertEquals(Tuple.from(10L), path2.get(3).getRemainder());
        }
    }

    @Test
    public void testInvalidPath() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root",  KeyType.STRING, "production")
                        .addSubdirectory(new KeySpaceDirectory("a", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING))));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            // Building a tuple in the correct order works
            Tuple tuple = root.path(context,"root")
                    .add("a", "foo")
                    .add("b", "bar").toTuple();
            assertEquals(Tuple.from("production", "foo", "bar"), tuple);

            // Walking in the wrong order fails
            assertThrows(NoSuchDirectoryException.class,
                    () -> root.path(context,"root").add("b", "foo").add("a", "bar").toTuple());
        }
    }

    @Test
    public void testAllTypesAnyValues() throws Exception {
        KeySpaceDirectory rootDir = new KeySpaceDirectory("root", KeyType.LONG, 1L);
        for (KeyTypeValue kv : valueOfEveryType) {
            rootDir.addSubdirectory(new KeySpaceDirectory(kv.keyType.toString(), kv.keyType));
        }

        KeySpace root = new KeySpace(rootDir);

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {

            for (KeyTypeValue kv : valueOfEveryType) {
                // Test that we can set a good type and get the right tuple back
                final Object value = kv.generator.get();
                assertEquals(Tuple.from(1L, value),
                        root.path(context, "root").add(kv.keyType.toString(), value).toTuple());

                final Object badValue = pickDifferentType(kv.keyType).generator.get();
                assertThrows(RecordCoreArgumentException.class,
                        () -> root.path(context, "root").add(kv.keyType.toString(), badValue).toTuple());
            }
        }
    }

    public KeyTypeValue pickDifferentType(KeyType keyType) {
        while (true) {
            KeyTypeValue kv = valueOfEveryType.get(random.nextInt(valueOfEveryType.size()));
            if (kv.keyType != keyType) {
                return kv;
            }
        }
    }

    @Test
    public void testAllTypesConstValues() throws Exception {
        KeySpaceDirectory rootDir = new KeySpaceDirectory("root", KeyType.LONG, 1L);
        for (KeyTypeValue kv : valueOfEveryType) {
            rootDir.addSubdirectory(new KeySpaceDirectory(kv.keyType.toString(), kv.keyType, kv.value));
        }

        KeySpace root = new KeySpace(rootDir);

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            // Test all constants that match the ones we created with
            for (KeyTypeValue kv : valueOfEveryType) {
                assertEquals(Tuple.from(1L, kv.value),
                        root.path(context, "root").add(kv.keyType.toString()).toTuple());
                assertEquals(Tuple.from(1L, kv.value),
                        root.path(context, "root").add(kv.keyType.toString(), kv.value).toTuple());

                // Try a different value of the same type and make sure that fails
                if (kv.keyType != KeyType.NULL) {
                    assertThrows(RecordCoreArgumentException.class,
                            () -> root.path(context, "root").add(kv.keyType.toString(), kv.value2).toTuple());
                }

                // Try a completely different type and make sure that fails
                final Object badValue = pickDifferentType(kv.keyType).generator.get();
                assertThrows(RecordCoreArgumentException.class,
                        () -> root.path(context, "root").add(kv.keyType.toString(), badValue).toTuple());
            }
        }

    }

    @Test
    public void testDirectoryLayerDirectoryUsingLongs() throws Exception {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("cabinet", "cabinet")
                        .addSubdirectory(new DirectoryLayerDirectory("game")));
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        final Tuple senetTuple;
        final Tuple urTuple;
        try (FDBRecordContext context = database.openContext()) {
            senetTuple = root.path(context, "cabinet").add("game", "senet").toTuple();
            urTuple = root.path(context, "cabinet").add("game", "royal_game_of_ur").toTuple();
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            // Verify that I can create the tuple again using the directory layer values.
            assertEquals(senetTuple, root.path(context, "cabinet")
                    .add("game", senetTuple.getLong(1)).toTuple());
            assertEquals(urTuple, root.path(context, "cabinet")
                    .add("game", urTuple.getLong(1)).toTuple());
        }
    }

    @Test
    public void testDirectoryLayerDirectoryValidation() throws Exception {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("school", "school")
                        .addSubdirectory(new DirectoryLayerDirectory("school_name")
                            .addSubdirectory(new DirectoryLayerDirectory("teachers", "teachers"))
                            .addSubdirectory(new DirectoryLayerDirectory("students", "students"))));
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        final Tuple teachersTuple;
        final Tuple studentsTuple;
        try (FDBRecordContext context = database.openContext()) {
            teachersTuple = root.path(context, "school").add("school_name", "Football Tech").add("teachers").toTuple();
            studentsTuple = root.path(context, "school").add("school_name", "Football Tech").add("students").toTuple();
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            // Use the wrong directory layer value for the students and teachers
            assertThrows(RecordCoreArgumentException.class,
                    () -> root.path(context, "school")
                            .add("school_name", "Football Tech")
                            .add("teachers", studentsTuple.getLong(1)).toTuple());
            assertThrows(RecordCoreArgumentException.class,
                    () -> root.path(context, "school")
                            .add("school_name", "Football Tech")
                            .add("students", teachersTuple.getLong(1)).toTuple());

            // Use a value that does not exist in the directory layer as the school name.
            assertThrows(NoSuchElementException.class,
                    () -> root.path(context, "school")
                            .add("school_name", -746464638L)
                            .add("teachers").toTuple());
        }
    }

    @Test
    public void testDirectoryLayerDirectoryWithMetadata() {
        String testRoot = "test-root-" + random.nextInt();
        ResolverCreateHooks hooks = new ResolverCreateHooks(DEFAULT_CHECK, DirWithMetadataWrapper::metadataHook);
        KeySpace root = rootForMetadataTests(testRoot, hooks, getGenerator());
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            String dir1 = "test-string-" + random.nextInt();
            String dir2 = "test-string-" + random.nextInt();
            KeySpacePath path1 = root.path(context, testRoot).add("dir_with_metadata_name", dir1);
            KeySpacePath path2 = root.path(context, testRoot).add("dir_with_metadata_name", dir2);

            assertThat("path gets wrapped", path1, is(instanceOf(DirWithMetadataWrapper.class)));
            assertThat("path gets wrapped", path2, is(instanceOf(DirWithMetadataWrapper.class)));
            DirWithMetadataWrapper wrapped1 = (DirWithMetadataWrapper) path1;
            DirWithMetadataWrapper wrapped2 = (DirWithMetadataWrapper) path2;
            assertArrayEquals(wrapped1.metadata().join(), Tuple.from(dir1, dir1.length()).pack());
            assertArrayEquals(wrapped2.metadata().join(), Tuple.from(dir2, dir2.length()).pack());
        }
    }

    @Test
    public void testMetadataFromLookupByKey() {
        String testRoot = "test-root-" + random.nextInt();
        ResolverCreateHooks hooks = new ResolverCreateHooks(DEFAULT_CHECK, DirWithMetadataWrapper::metadataHook);
        KeySpace root = rootForMetadataTests(testRoot, hooks, getGenerator());
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        Tuple tuple;
        String dir = "test-string-" + random.nextInt();
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath path1 = root.path(context, testRoot).add("dir_with_metadata_name", dir);
            tuple = path1.toTuple();
            context.ensureActive().set(tuple.pack(), Tuple.from(0).pack());

            DirWithMetadataWrapper wrapped = (DirWithMetadataWrapper) path1;
            assertArrayEquals(wrapped.metadata().join(), DirWithMetadataWrapper.metadataHook(dir));
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            DirWithMetadataWrapper fromPath = (DirWithMetadataWrapper) root.pathFromKey(context, tuple);
            assertArrayEquals(fromPath.metadata().join(), DirWithMetadataWrapper.metadataHook(dir));
        }
    }

    @Test
    public void testSeesMetadataUpdates() {
        String testRoot = "test-root-" + random.nextInt();
        Function<FDBRecordContext, CompletableFuture<LocatableResolver>> generator = getGenerator();
        KeySpace root = rootForMetadataTests(testRoot, generator);
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        database.setResolverStateRefreshTimeMillis(100);
        String dir = "test-string-" + random.nextInt();
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath path1 = root.path(context, testRoot).add("dir_with_metadata_name", dir);

            DirWithMetadataWrapper wrapped = (DirWithMetadataWrapper) path1;
            assertThat("there's no metadata", wrapped.metadata().join(), is(nullValue()));
        }

        try (FDBRecordContext context = database.openContext()) {
            generator.apply(context)
                    .thenCompose(scope -> scope.updateMetadataAndVersion(dir, Tuple.from("new-metadata").pack()))
                    .join();
        }

        eventually("we see the new metadata for the path", () -> {
            try (FDBRecordContext context = database.openContext()) {
                KeySpacePath path1 = root.path(context, testRoot).add("dir_with_metadata_name", dir);
                return ((DirWithMetadataWrapper) path1).metadata().join();
            }
        }, is(Tuple.from("new-metadata").pack()), 120, 10);
    }

    private Function<FDBRecordContext, CompletableFuture<LocatableResolver>> getGenerator() {
        String tmpDirLayer = "tmp-dir-layer-" + random.nextLong();
        KeySpace dirLayerKeySpace = new KeySpace(new KeySpaceDirectory(tmpDirLayer, KeyType.STRING, tmpDirLayer));
        return context ->
                CompletableFuture.completedFuture(new ScopedInterningLayer(dirLayerKeySpace.path(context, tmpDirLayer)));
    }

    private KeySpace rootForMetadataTests(String name,
                                          Function<FDBRecordContext, CompletableFuture<LocatableResolver>> generator) {
        return rootForMetadataTests(name, ResolverCreateHooks.getDefault(), generator);
    }

    private KeySpace rootForMetadataTests(String name,
                                          ResolverCreateHooks hooks,
                                          Function<FDBRecordContext, CompletableFuture<LocatableResolver>> generator) {
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();


        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory(name, name)
                        .addSubdirectory(new DirectoryLayerDirectory("dir_with_metadata_name", DirWithMetadataWrapper::new, generator, hooks))
        );

        database.run(context -> root.path(context, name).deleteAllDataAsync());
        return root;
    }

    private static class DirWithMetadataWrapper extends KeySpacePathWrapper {
        DirWithMetadataWrapper(KeySpacePath inner) {
            super(inner);
        }

        static byte[] metadataHook(String schoolName) {
            return Tuple.from(schoolName, schoolName.length()).pack();
        }

        CompletableFuture<byte[]> metadata() {
            return inner.getResolvedPathMetadata();
        }
    }

    @Test
    public void testCustomDirectoryResolver() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.LONG, 1L)
                        .addSubdirectory(new ConstantResolvingKeySpaceDirectory("toString", KeyType.STRING, KeySpaceDirectory.ANY_VALUE, v -> "val" + v.toString()))
                        .addSubdirectory(new ConstantResolvingKeySpaceDirectory("toWrongType", KeyType.LONG, KeySpaceDirectory.ANY_VALUE, v -> "val" + v.toString())));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            assertEquals(Tuple.from(1L, "val15"),
                    root.path(context, "root").add("toString", 15).toTuple());
            assertThrows(RecordCoreArgumentException.class,
                    () -> root.path(context,"root").add("toWrongType", 21L).toTuple());
        }
    }

    @Test
    public void testFromTupleWithConstantValue() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.LONG, 1L)
                    .addSubdirectory(new KeySpaceDirectory("dir1", KeyType.STRING, "a"))
                    .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.STRING, "b"))
                    .addSubdirectory(new KeySpaceDirectory("dir3", KeyType.LONG)));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Tuple tuple = Tuple.from(1L, "a");
            assertEquals(tuple, root.pathFromKey(context, tuple).toTuple());
            tuple = Tuple.from(1L, "b");
            assertEquals(tuple, root.pathFromKey(context, tuple).toTuple());
            final Tuple badTuple1 = Tuple.from(1L, "c", "d");
            assertThrows(RecordCoreArgumentException.class, () -> root.pathFromKey(context, badTuple1).toTuple(),
                    "key_tuple", badTuple1,
                    "key_tuple_pos", 1);
        }
    }

    @Test
    public void testPathToString() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.LONG)
                        .addSubdirectory(new KeySpaceDirectory("dir1", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.BYTES)))
                        .addSubdirectory(new KeySpaceDirectory("dir3", KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("dir4"))));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        final Long fooValue;
        final Long barValue;
        try (FDBRecordContext context = database.openContext()) {
            List<Long> entries = resolveBatch(context, "_foo", "_bar");
            context.commit();
            fooValue = entries.get(0);
            barValue = entries.get(1);
        }

        try (FDBRecordContext context = database.openContext()) {
            assertEquals("/root:4/dir1:hi/dir2:0x4142+(\"blah\")",
                    root.pathFromKey(context, Tuple.from(4L, "hi", new byte[] { 0x41, 0x42 }, "blah")).toString());
            assertEquals("/root:11", root.pathFromKey(context, Tuple.from(11L)).toString());
            assertEquals("/root:14/dir3:4/dir4:" + barValue + "->_bar", root.pathFromKey(context, Tuple.from(14L, 4L, barValue)).toString());

            assertEquals("/root:11/dir3:17/dir4:" + fooValue,
                    root.path(context, "root", 11L)
                            .add("dir3", 17L)
                            .add("dir4", fooValue).toString());
            KeySpacePath path = root.path(context, "root", 11L)
                            .add("dir3", 17L).add("dir4", "_foo");
            // path.toTuple() forces the directory layer value to resolve
            path.toTuple();
            assertEquals("/root:11/dir3:17/dir4:" + fooValue + "->_foo", path.toString());
        }
    }

    @Test
    public void testListDoesNotGoTooDeep() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("a", KeyType.LONG, random.nextLong())
                    .addSubdirectory(new DirectoryLayerDirectory("b")
                            .addSubdirectory(new KeySpaceDirectory("c", KeyType.STRING)
                                    .addSubdirectory(new KeySpaceDirectory("d", KeyType.BYTES)
                                        .addSubdirectory(new KeySpaceDirectory("e", KeyType.LONG))))));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (int i = 0; i < 5; i++) {
                tr.set(root.path(context, "a")
                        .add("b", "foo_" + i)
                        .add("c", "hi_" + i)
                        .add("d", new byte[] { (byte) i } ).toTuple().pack(), Tuple.from(i).pack());
            }
            context.commit();
        }

        // Even though the keyspace understands directories "a"/"b"/"c" we want to make sure that if you
        // list all "b" directories you only get a path leading up to the "b"'s, wth the "c" values as a
        // remainder on the "b" path entry.
        try (FDBRecordContext context = database.openContext()) {
            List<KeySpacePath> paths;

            // Check listing from the root
            paths = root.list(context, "a");
            assertThat("Number of paths in 'a'", paths.size(), is(1));
            assertThat("Value of subdirectory 'a'", paths.get(0).getValue(), is(root.getDirectory("a").getValue()));
            assertThat("Remainder size of 'a'", paths.get(0).getRemainder().size(), is(3));

            // List from "b"
            paths = root.path(context, "a").list("b");
            assertThat("Number of paths in 'b'", paths.size(), is(5));
            for (KeySpacePath path : paths) {
                assertThat("Listing of 'b' directory", path.getDirectoryName(), is("b"));
                Tuple remainder = path.getRemainder();
                assertThat("Remainder of 'b'", remainder.size(), is(2));
                assertThat("Remainder of 'b', first tuple value", remainder.getString(0), startsWith("hi_"));
                assertThat("Remainder of 'b', second tuple value", remainder.getBytes(1), instanceOf(new byte[0].getClass()));
            }

            // List from "c"
            paths = root.path(context, "a").add("b", "foo_0").list("c");
            assertThat("Number of paths in 'c'", paths.size(), is(1));
            for (KeySpacePath path  : paths) {
                assertThat("Listing of 'c' directory", path.getDirectoryName(), is("c"));
                final Tuple remainder = path.getRemainder();
                assertThat("Remainder of 'c'", remainder.size(), is(1));
                assertThat("Remainder of 'c', first tuple value", remainder.getBytes(0), instanceOf(new byte[0].getClass()));
            }

            // List from "d"
            paths = root.path(context, "a").add("b", "foo_0").add("c", "hi_0").list("d");
            assertThat("Number of paths in 'd'", paths.size(), is(1));
            KeySpacePath path = paths.get(0);
            assertThat("Remainder of 'd'", path.getRemainder(), is((Tuple) null));

            // List from "e" (which has no data)
            paths = root.path(context, "a").add("b", "foo_0").add("c", "hi_0").add("d", new byte[] { 0x00 }).list("e");
            assertThat("Number of paths in 'e'", paths.size(), is(0));
        }
    }

    @Test
    public void testDeleteAllDataAndHasData() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.LONG, Math.abs(random.nextLong()))
                        .addSubdirectory(new KeySpaceDirectory("dir1", KeyType.STRING, "a")
                                .addSubdirectory(new KeySpaceDirectory("dir1_1", KeyType.LONG)))
                        .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.STRING, "b")
                                .addSubdirectory(new KeySpaceDirectory("dir2_1", KeyType.LONG)))
                        .addSubdirectory(new KeySpaceDirectory("dir3", KeyType.STRING, "c")
                                .addSubdirectory(new KeySpaceDirectory("dir3_1", KeyType.LONG))));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (int i = 0; i < 5; i++) {
                tr.set(root.path(context, "root").add("dir1").add("dir1_1", i).toTuple().pack(), Tuple.from(i).pack());
                tr.set(root.path(context, "root").add("dir2").add("dir2_1", i).toTuple().pack(), Tuple.from(i).pack());
                tr.set(root.path(context, "root").add("dir3").add("dir3_1", i).toTuple().pack(), Tuple.from(i).pack());
            }
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            // All directories hava data?
            for (int i = 1; i <= 3; i++) {
                assertTrue(root.path(context, "root").add("dir" + i).hasData(), "dir" + i + " is empty!");
            }
            // Clear out dir2
            root.path(context, "root").add("dir2").deleteAllData();
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            assertTrue(root.path(context, "root").add("dir1").hasData(), "dir1 is empty!");
            assertFalse(root.path(context, "root").add("dir2").hasData(), "dir2 has data!");
            assertTrue(root.path(context, "root").add("dir3").hasData(), "dir3 is empty!");
        }
    }

    @Test
    public void testListAnyValue() throws Exception {
        // Create a root directory called "a" with subdirs of every type (no constants for now)
        Long rootValue = random.nextLong();
        KeySpaceDirectory dirA = new KeySpaceDirectory("a", KeyType.LONG, rootValue);
        for (KeyTypeValue kv : valueOfEveryType) {
            dirA.addSubdirectory(new KeySpaceDirectory(kv.keyType.toString(), kv.keyType));
        }
        KeySpace root = new KeySpace(dirA);

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();

        final Map<KeyType, List<Tuple>> valuesForType = new HashMap<>();

        // Create an entry in the keyspace with a row for every type that we support
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (KeyTypeValue kv : valueOfEveryType) {
                List<Tuple> values = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    Object value = kv.generator.get();
                    Tuple tupleValue = Tuple.from(value);
                    if (! values.contains(tupleValue)) {
                        values.add(tupleValue);

                        // Make sure that we have extra values in the same keyspace that don't get included in the
                        // final results.
                        for (int j = 0; j < 5; j++) {
                            tr.set(Tuple.from(rootValue, value, j).pack(), Tuple.from(i).pack());
                        }
                    }
                }
                valuesForType.put(kv.keyType, values);
            }
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            for (KeyTypeValue kv : valueOfEveryType) {
                List<KeySpacePath> paths = root.path(context, "a").list(kv.keyType.toString());
                List<Tuple> values = valuesForType.get(kv.keyType);
                assertEquals(values.size(), paths.size());

                for (KeySpacePath path : paths) {
                    Tuple tuple = path.toTuple();
                    assertTrue(values.remove(Tuple.from(tuple.get(1))), kv.keyType + " missing: " + tuple.get(1));
                }

                assertTrue(values.isEmpty(), "Missing values of type: " + kv.keyType + ": " + values);
            }
        }
    }

    @Test
    public void testListAcrossTransactions() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("a", KeyType.LONG, random.nextLong())
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING)));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        final List<String> directoryEntries = IntStream.range(0, 10).boxed().map(i -> "val_" + i).collect(Collectors.toList());

        final KeySpacePath rootPath;
        try (final FDBRecordContext context = database.openContext()) {
            rootPath = root.path(context, "a");
            final Transaction tr = context.ensureActive();
            directoryEntries.forEach(name -> tr.set(rootPath.add("b",  name).toTuple().pack(), TupleHelpers.EMPTY.pack()));
            context.commit();
        }

        byte[] continuation = null;
        int idx = 0;
        do {
            try (final FDBRecordContext context = database.openContext()) {
                final KeySpacePath rootWithNewContext = rootPath.copyWithNewContext(context);
                final RecordCursor<KeySpacePath>  cursor = rootWithNewContext.listAsync("b", continuation,
                        new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(2).build()));
                List<KeySpacePath> subdirs = context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST, cursor.asList());
                if (!subdirs.isEmpty()) {
                    assertEquals(2, subdirs.size(), "Wrong number of path entries returned");
                    assertEquals("val_" + idx, subdirs.get(0).getResolvedValue().get());
                    assertEquals("val_" + (idx + 1), subdirs.get(1).getResolvedValue().get());
                    idx += 2;
                    continuation = cursor.getContinuation();
                    System.out.println(continuation == null ? "null" : Tuple.fromBytes(continuation));
                } else {
                    continuation = cursor.getContinuation();
                    assertNull(cursor.getContinuation());
                }
            }
        } while (continuation != null);

        assertEquals(directoryEntries.size(), idx);
    }

    @Test
    public void testPathCopyRetainsWrapper() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("a", KeyType.LONG, random.nextLong(), TestWrapper1::new)
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING, TestWrapper2::new)));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        KeySpacePath path;
        try (final FDBRecordContext context = database.openContext()) {
            path = root.path(context, "a");
            assertTrue(path instanceof TestWrapper1, "root isn't a testWrapper1");
            path = path.add("b", 10);
            assertTrue(path instanceof TestWrapper2, "b isn't a testWrapper2");
        }

        try (final FDBRecordContext context = database.openContext()) {
            KeySpacePath copyPath = path.copyWithNewContext(context);
            assertTrue(copyPath instanceof TestWrapper2, "b isn't a testWrapper2");
            assertTrue(copyPath.getParent() instanceof TestWrapper1, "root isn't a testWrapper1");
        }
    }

    private static class TestWrapper1 extends KeySpacePathWrapper {
        public TestWrapper1(KeySpacePath inner) {
            super(inner);
        }
    }

    private static class TestWrapper2 extends KeySpacePathWrapper {
        public TestWrapper2(KeySpacePath inner) {
            super(inner);
        }
    }

    @Test
    public void testListConstantValue() throws Exception {
        // Create a root directory called "a" with subdirs of every type and a constant value
        Long rootValue = random.nextLong();
        KeySpaceDirectory dirA = new KeySpaceDirectory("a", KeyType.LONG, rootValue);
        for (KeyTypeValue kv : valueOfEveryType) {
            dirA.addSubdirectory(new KeySpaceDirectory(kv.keyType.toString(), kv.keyType, kv.generator.get()));
        }
        KeySpace root = new KeySpace(dirA);

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            for (KeyTypeValue kv : valueOfEveryType) {
                KeySpaceDirectory dir = root.getDirectory("a").getSubdirectory(kv.keyType.name());
                for (int i = 0; i < 5; i++) {
                    tr.set(Tuple.from(rootValue, dir.getValue(), i).pack(), Tuple.from(i).pack());
                }
            }
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            for (KeyTypeValue kv : valueOfEveryType) {
                KeySpaceDirectory dir = root.getDirectory("a").getSubdirectory(kv.keyType.name());

                List<KeySpacePath> paths = root.path(context, "a").list(kv.keyType.toString());
                assertEquals(1, paths.size());
                if (dir.getKeyType() == KeyType.BYTES) {
                    assertTrue(Arrays.equals((byte[]) dir.getValue(), paths.get(0).toTuple().getBytes(1)));
                } else {
                    assertEquals(dir.getValue(), paths.get(0).toTuple().get(1));
                }
            }
        }
    }

    @Test
    public void testListDirectoryLayer() throws Exception {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("a", KeyType.LONG, random.nextLong())
                        .addSubdirectory(new DirectoryLayerDirectory("b"))
                        .addSubdirectory(new KeySpaceDirectory("c", KeyType.STRING, "c")
                                .addSubdirectory(new DirectoryLayerDirectory("d", "d"))
                                .addSubdirectory(new DirectoryLayerDirectory("e", "e"))
                                .addSubdirectory(new DirectoryLayerDirectory("f", "f"))));

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 5; j++) {
                    Tuple key = root.path(context, "a").add("b", "value_" + i).toTuple().add(i).add(j);
                    tr.set(key.pack(), Tuple.from(i).pack());
                }
            }

            for (int i = 0; i < 5; i++) {
                tr.set(root.path(context, "a").add("c").add("d").toTuple().add(i).pack(), Tuple.from(i).pack());
                tr.set(root.path(context, "a").add("c").add("e").toTuple().add(i).pack(), Tuple.from(i).pack());
                tr.set(root.path(context, "a").add("c").add("f").toTuple().add(i).pack(), Tuple.from(i).pack());
            }

            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            List<KeySpacePath> paths = root.path(context, "a").list("b");
            assertEquals(10, paths.size());

            for (KeySpacePath path : paths) {
                final long index = path.getRemainder().getLong(0); // The first part of the remainder was the index
                // We should always get the "first" key for the value in the directory
                assertEquals(0, path.getRemainder().getLong(1));
                assertTrue(index >= 0 && index < 10);
                assertEquals("a", path.getParent().getDirectoryName());
                assertEquals(root.getDirectory("a").getValue(), path.getParent().getValue());
                assertEquals("value_" + index, path.getValue());
            }

            for (String subdir : ImmutableList.of("d", "e", "f")) {
                paths = root.path(context, "a").add("c").list(subdir);
                assertEquals(1, paths.size());
                assertEquals(subdir, paths.get(0).getValue());
                assertEquals(0L, paths.get(0).getRemainder().getLong(0));
                assertEquals("c", paths.get(0).getParent().getDirectoryName());
                assertEquals("a", paths.get(0).getParent().getParent().getDirectoryName());
            }
        }
    }

    /*
     * This isn't specifically just a test, but is also here to demonstrate how you can use the KeySpacePath
     * wrapping facility to work with paths in a type safe manner.
     */
    @Test
    public void testPathWrapperExample() throws Exception {
        EnvironmentKeySpace keySpace = new EnvironmentKeySpace("production");
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();

        // Create a tuple to represent a path to a user's main store. This will trigger the creation of the
        // necessary directory layer entries.
        final Tuple dataStoreTuple;
        final Tuple metadataStoreTuple;
        try (FDBRecordContext context = database.openContext()) {
            ApplicationPath application = keySpace.root(context).userid(123).application("myApplication");
            dataStoreTuple = application.dataStore().toTuple();
            metadataStoreTuple = application.metadataStore().toTuple();
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            List<Long> entries = resolveBatch(context, keySpace.getRootName(), "myApplication");

            // Validate the entries created above look like what we expect
            assertEquals(Tuple.from(entries.get(0), 123L, entries.get(1), EnvironmentKeySpace.DATA_VALUE), dataStoreTuple);
            assertEquals(Tuple.from(entries.get(0), 123L, entries.get(1), EnvironmentKeySpace.METADATA_VALUE), metadataStoreTuple);

            KeySpacePath path =  keySpace.fromKey(context, dataStoreTuple);
            assertThat(path, instanceOf(DataPath.class));

            DataPath mainStorePath = (DataPath) path;
            assertEquals(EnvironmentKeySpace.DATA_VALUE, mainStorePath.getValue());
            assertEquals(EnvironmentKeySpace.DATA_VALUE, mainStorePath.getResolvedValue().get());
            assertEquals(entries.get(1), mainStorePath.parent().getResolvedValue().get());
            assertEquals("myApplication", mainStorePath.parent().getValue());
            assertEquals(123L, mainStorePath.parent().parent().getValue());
            assertEquals(entries.get(0), mainStorePath.parent().parent().parent().getResolvedValue().get());
            assertEquals("production", mainStorePath.parent().parent().parent().getValue());
            assertEquals(null, mainStorePath.parent().parent().parent().parent());

            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 1)), instanceOf(EnvironmentRoot.class));
            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 2)), instanceOf(UserPath.class));
            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 3)), instanceOf(ApplicationPath.class));

            path = keySpace.fromKey(context, metadataStoreTuple);
            assertThat(path, instanceOf(MetadataPath.class));

            MetadataPath metadataPath = (MetadataPath) path;
            assertEquals(EnvironmentKeySpace.METADATA_VALUE, metadataPath.getValue());
            assertEquals(EnvironmentKeySpace.METADATA_VALUE, metadataPath.getResolvedValue().get());
            assertEquals(entries.get(1), metadataPath.parent().getResolvedValue().get());
            assertEquals("myApplication", metadataPath.parent().getValue());
            assertEquals(123L, metadataPath.parent().parent().getValue());
            assertEquals(entries.get(0), metadataPath.parent().parent().parent().getResolvedValue().get());
            assertEquals("production", metadataPath.parent().parent().parent().getValue());
            assertEquals(null, metadataPath.parent().parent().parent().parent());

            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 1)), instanceOf(EnvironmentRoot.class));
            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 2)), instanceOf(UserPath.class));
            assertThat(keySpace.fromKey(context, TupleHelpers.subTuple(dataStoreTuple, 0, 3)), instanceOf(ApplicationPath.class));

            // Create a fake main store "record" key to demonstrate that we can get the key as the remainder
            Tuple recordTuple = dataStoreTuple.add(1L).add("someStr").add(0L); // 1=record space, record id, 0=unsplit record
            path =  keySpace.fromKey(context, recordTuple);
            assertThat(path, instanceOf(DataPath.class));
            assertEquals(Tuple.from(1L, "someStr", 0L), path.getRemainder());
            assertEquals(dataStoreTuple, path.toTuple());
        }
    }

    // This isn't so much a test as validation to ensure that the code and output that is used in the comments
    // in some of the implementing classes works as advertised.
    @Test
    public void testToTree() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("state", KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("office_id", KeyType.LONG)
                                .addSubdirectory(new KeySpaceDirectory("employees", KeyType.STRING, "E")
                                        .addSubdirectory(new KeySpaceDirectory("employee_id", KeyType.LONG)))
                                .addSubdirectory(new KeySpaceDirectory("inventory", KeyType.STRING, "I")
                                        .addSubdirectory(new KeySpaceDirectory("stock_id", KeyType.LONG)))
                                .addSubdirectory(new KeySpaceDirectory("sales", KeyType.STRING, "S")
                                        .addSubdirectory(new KeySpaceDirectory("transaction_id", KeyType.UUID))
                                        .addSubdirectory(new KeySpaceDirectory("layaways", KeyType.NULL)
                                                .addSubdirectory(new KeySpaceDirectory("transaction_id", KeyType.UUID))))));
        System.out.println(keySpace.toString());
    }

    @Test
    public void testAddToPathPreservesParentWrapper() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("a", KeyType.STRING, PathA::new)
                    .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING, PathB::new))
                    .addSubdirectory(new DirectoryLayerDirectory("c", PathC::new)));
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            PathA a = (PathA) keySpace.path(context, "a", "foo");
            PathB b = (PathB) a.add("b", "bar");
            PathC c = (PathC) a.add("c", "bax");
            assertThat("parent of b should be a PathA", b.getParent(), instanceOf(PathA.class));
            assertThat("parent of c should be a PathA", c.getParent(), instanceOf(PathA.class));
        }
    }

    @Test
    public void testCopyPreservesWrapper() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("a", KeyType.STRING, PathA::new)
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING, PathB::new))
                        .addSubdirectory(new DirectoryLayerDirectory("c", PathC::new)));
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            final PathA a = (PathA) keySpace.path(context, "a", "foo");
            final PathB b = (PathB) a.add("b", "bar");
            final PathC c = (PathC) a.add("c", "bax");

            KeySpacePath copy = a.copyWithNewContext(context);
            assertThat("copy of 'a' is not instanceof PathA", copy, instanceOf(PathA.class));
            copy = b.copyWithNewContext(context);
            assertThat("copy of 'b' is not instanceof PathB", copy, instanceOf(PathB.class));
            assertThat("copy of 'b' parent is not instanceof PathA", copy.getParent(), instanceOf(PathA.class));
            copy = c.copyWithNewContext(context);
            assertThat("copy of 'c' is not instanceof PathC", copy, instanceOf(PathC.class));
            assertThat("copy of 'c' parent is not instanceof PathA", copy.getParent(), instanceOf(PathA.class));
        }
    }

    @Test
    public void testListPreservesWrapper() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("a", KeyType.STRING, PathA::new)
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING, PathB::new)));
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            PathA root = (PathA) keySpace.path(context, "a", "foo");
            tr.set(root.add("b", "one").toTuple().pack(), TupleHelpers.EMPTY.pack());
            tr.set(root.add("b", "two").toTuple().pack(), TupleHelpers.EMPTY.pack());
            tr.set(root.add("b", "three").toTuple().pack(), TupleHelpers.EMPTY.pack());
            tr.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            List<KeySpacePath> paths = keySpace.path(context, "a", "foo").list("b");
            for (KeySpacePath path : paths) {
                assertThat("Path should be PathB", path, instanceOf(PathB.class));
                assertThat("parent should be PathA", path.getParent(), instanceOf(PathA.class));
            }
        }
    }

    @Test
    public void flattenPreservesWrapper() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("a", KeyType.STRING, PathA::new)
                        .addSubdirectory(new KeySpaceDirectory("b", KeyType.STRING, PathB::new)));
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            List<KeySpacePath> path = keySpace.path(context, "a", "foo").add("b", "bar").flatten();
            assertThat("a should be pathA", path.get(0), instanceOf(PathA.class));
            assertThat("b should be pathB", path.get(1), instanceOf(PathB.class));
        }
    }

    /** Used to validate wrapping of path names. */
    public static class PathA extends KeySpacePathWrapper {
        public PathA(KeySpacePath parent) {
            super(parent);
        }
    }

    /** Used to validate wrapping of path names. */
    public static class PathB extends KeySpacePathWrapper {
        public PathB(KeySpacePath parent) {
            super(parent);
        }
    }

    /** Used to validate wrapping of path names. */
    public static class PathC extends KeySpacePathWrapper {
        public PathC(KeySpacePath parent) {
            super(parent);
        }
    }

    private List<Long> resolveBatch(FDBRecordContext context, String... names) {
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (String name : names) {
            futures.add(ScopedDirectoryLayer.global(context.getDatabase()).resolve(context.getTimer(), name));
        }
        return AsyncUtil.getAll(futures).join();
    }

    private static class ConstantResolvingKeySpaceDirectory extends KeySpaceDirectory {

        private final Function<Object, Object> resolver;

        public ConstantResolvingKeySpaceDirectory(String name, KeyType keyType, Object constantValue, Function<Object, Object> resolver) {
            super(name, keyType, constantValue);
            this.resolver = resolver;
        }

        @Override
        protected CompletableFuture<PathValue> toTupleValueAsync(FDBRecordContext context, Object value) {
            return CompletableFuture.completedFuture(new PathValue(resolver.apply(value)));
        }
    }

    /**
     * This provides an example of a way in which you can define a KeySpace in a relatively clean and type-safe
     * manner. It defines a keyspace that looks like:
     * <pre>
     *    [environment]           - A string the identifies the logical environment (like prod, test, qa, etc.).
     *      |                       This string is converted by the directory layer as a small integer value.
     *      +- userid             - An integer ID for each user in the system
     *         |
     *         +- [application]   - Tne name of an application the user runs (again, converted by the directory
     *            |                 layer into a small integer value)
     *            +- data=1       - Constant value of "1", which is the location of a {@link FDBRecordStore}
     *            |                 in which application data is to be stored
     *            +- metadata=2   - Constant value of "2", which is the Location of another <code>FDBRecordStore</code>
     *                              in which application metadata or configuration information can live.
     * </pre>
     * The main point of this class is to demonstrate how you can use the KeySpacePath wrapping facility to provide
     * implementations of the path elements that are meaningful to your application environment and type safe.
     */
    private static class EnvironmentKeySpace {
        private final KeySpace root;
        private final String rootName;

        public static String USER_KEY = "userid";
        public static String APPLICATION_KEY = "application";
        public static String DATA_KEY = "data";
        public static long DATA_VALUE = 1L;
        public static String METADATA_KEY = "metadata";
        public static long METADATA_VALUE = 2L;

        /**
         * The <code>EnvironmentKeySpace</code> scopes all of the data it stores underneath of a <code>rootName</code>,
         * for example, you could define an instance for <code>prod</code>, <code>test</code>, <code>qa</code>, etc.
         *
         * @param rootName The root name underwhich all data is stored.
         */
        public EnvironmentKeySpace(String rootName) {
            this.rootName = rootName;
            root = new KeySpace(
                    new DirectoryLayerDirectory(rootName, rootName, EnvironmentRoot::new)
                            .addSubdirectory(new KeySpaceDirectory(USER_KEY, KeyType.LONG, UserPath::new)
                                    .addSubdirectory(new DirectoryLayerDirectory(APPLICATION_KEY, ApplicationPath::new)
                                            .addSubdirectory(new KeySpaceDirectory(DATA_KEY, KeyType.LONG, DATA_VALUE, DataPath::new))
                                            .addSubdirectory(new KeySpaceDirectory(METADATA_KEY, KeyType.LONG, METADATA_VALUE, MetadataPath::new)))));
        }

        public String getRootName() {
            return rootName;
        }

        /**
         * Returns an implementation of a <code>KeySpacePath</code> that represents the start of the environment.
         */
        public EnvironmentRoot root(FDBRecordContext context)  {
            return (EnvironmentRoot) root.path(context, rootName);
        }

        /**
         * Given a tuple that represents an FDB key that came from this KeySpace, returns the leaf-most path
         * element in which the tuple resides.
         */
        public KeySpacePath fromKey(FDBRecordContext context, Tuple tuple) {
            return root.pathFromKey(context, tuple);
        }
    }

    /**
     * A <code>KeySpacePath</code> that represents the logical root of the environment.
     */
    private static class EnvironmentRoot extends KeySpacePathWrapper {
        public EnvironmentRoot(KeySpacePath path) {
            super(path);
        }

        public KeySpacePath parent() {
            return null;
        }

        public UserPath userid(long userid) {
            return (UserPath) inner.add(EnvironmentKeySpace.USER_KEY, userid);
        }
    }

    private static class UserPath extends KeySpacePathWrapper {
        public UserPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath application(String applicationName) {
            return (ApplicationPath) inner.add(EnvironmentKeySpace.APPLICATION_KEY, applicationName);
        }

        public EnvironmentRoot parent() {
            return (EnvironmentRoot) inner.getParent();
        }
    }

    private static class ApplicationPath extends KeySpacePathWrapper {
        public ApplicationPath(KeySpacePath path) {
            super(path);
        }

        public DataPath dataStore() {
            return (DataPath) inner.add(EnvironmentKeySpace.DATA_KEY);
        }

        public MetadataPath metadataStore() {
            return (MetadataPath) inner.add(EnvironmentKeySpace.METADATA_KEY);
        }

        public UserPath parent() {
            return (UserPath) inner.getParent();
        }
    }

    private static class DataPath extends KeySpacePathWrapper {
        public DataPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath parent() {
            return (ApplicationPath) inner.getParent();
        }
    }

    private static class MetadataPath extends KeySpacePathWrapper {
        public MetadataPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath parent() {
            return (ApplicationPath) inner.getParent();
        }
    }
}