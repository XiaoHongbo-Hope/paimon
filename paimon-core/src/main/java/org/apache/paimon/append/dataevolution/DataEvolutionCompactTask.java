/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.RecordWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Data evolution table compaction task. */
public class DataEvolutionCompactTask extends AppendCompactTask {

    private static final Map<String, String> DYNAMIC_WRITE_OPTIONS =
            Collections.singletonMap(CoreOptions.TARGET_FILE_SIZE.key(), "99999 G");

    private final boolean blobTask;

    public DataEvolutionCompactTask(
            BinaryRow partition, List<DataFileMeta> files, boolean blobTask) {
        super(partition, files);
        this.blobTask = blobTask;
    }

    public boolean isBlobTask() {
        return blobTask;
    }

    public CommitMessage doCompact(FileStoreTable table, String commitUser) throws Exception {
        if (blobTask) {
            return doCompactBlob(table, commitUser);
        }

        table = table.copy(DYNAMIC_WRITE_OPTIONS);
        long firstRowId = compactBefore.get(0).nonNullFirstRowId();

        RowType readWriteType =
                new RowType(
                        table.rowType().getFields().stream()
                                .filter(f -> f.type().getTypeRoot() != DataTypeRoot.BLOB)
                                .collect(Collectors.toList()));
        FileStorePathFactory pathFactory = table.store().pathFactory();
        AppendOnlyFileStore store = (AppendOnlyFileStore) table.store();

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(compactBefore)
                        .withBucketPath(pathFactory.bucketPath(partition, 0).toString())
                        .rawConvertible(false)
                        .build();
        RecordReader<InternalRow> reader =
                store.newDataEvolutionRead().withReadType(readWriteType).createReader(dataSplit);
        AppendFileStoreWrite storeWrite = (AppendFileStoreWrite) store.newWrite(commitUser);
        storeWrite.withWriteType(readWriteType);
        RecordWriter<InternalRow> writer = storeWrite.createWriter(partition, 0);

        reader.forEachRemaining(
                row -> {
                    try {
                        writer.write(row);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        List<DataFileMeta> writeResult = writer.prepareCommit(false).newFilesIncrement().newFiles();
        checkArgument(
                writeResult.size() == 1, "Data evolution compaction should produce one file.");

        DataFileMeta dataFileMeta = writeResult.get(0).assignFirstRowId(firstRowId);
        long minSequenceId =
                compactBefore.stream()
                        .mapToLong(DataFileMeta::minSequenceNumber)
                        .min()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Cannot get min sequence id from compact before files."));
        long maxSequenceId =
                compactBefore.stream()
                        .mapToLong(DataFileMeta::maxSequenceNumber)
                        .max()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Cannot get max sequence id from compact before files."));
        dataFileMeta = dataFileMeta.assignSequenceNumber(minSequenceId, maxSequenceId);
        compactAfter.add(dataFileMeta);

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        compactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        return new CommitMessageImpl(
                partition, 0, null, DataIncrement.emptyIncrement(), compactIncrement);
    }

    private CommitMessage doCompactBlob(FileStoreTable table, String commitUser) throws Exception {
        table = table.copy(DYNAMIC_WRITE_OPTIONS);
        FileStorePathFactory pathFactory = table.store().pathFactory();
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, 0);
        FileIO fileIO = table.fileIO();
        long schemaId = table.schema().id();

        // Group blob files by field name
        Map<String, List<DataFileMeta>> filesByField = new LinkedHashMap<>();
        for (DataFileMeta file : compactBefore) {
            String fieldName = file.writeCols().get(0);
            filesByField.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(file);
        }

        // For each blob field, sort by firstRowId and compact into one file
        for (Map.Entry<String, List<DataFileMeta>> entry : filesByField.entrySet()) {
            String fieldName = entry.getKey();
            List<DataFileMeta> fieldFiles = entry.getValue();
            fieldFiles.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));

            long firstRowId = fieldFiles.get(0).nonNullFirstRowId();
            long minSeq =
                    fieldFiles.stream()
                            .mapToLong(DataFileMeta::minSequenceNumber)
                            .min()
                            .getAsLong();
            long maxSeq =
                    fieldFiles.stream()
                            .mapToLong(DataFileMeta::maxSequenceNumber)
                            .max()
                            .getAsLong();

            RowType blobFieldType = table.rowType().project(fieldName);
            BlobFileFormat blobFileFormat = new BlobFileFormat();
            Path outputPath = dataFilePathFactory.newBlobPath();

            RowDataFileWriter writer =
                    new RowDataFileWriter(
                            fileIO,
                            RollingFileWriter.createFileWriterContext(
                                    blobFileFormat,
                                    blobFieldType,
                                    new SimpleColStatsCollector.Factory[] {
                                        NoneSimpleColStatsCollector::new
                                    },
                                    "none"),
                            outputPath,
                            blobFieldType,
                            schemaId,
                            () -> new LongCounter(0),
                            new FileIndexOptions(),
                            FileSource.COMPACT,
                            false,
                            false,
                            dataFilePathFactory.isExternalPath(),
                            Collections.singletonList(fieldName));

            // Read each blob file and write to the new file
            FormatReaderFactory readerFactory =
                    blobFileFormat.createReaderFactory(blobFieldType, blobFieldType, null);
            try {
                for (DataFileMeta file : fieldFiles) {
                    Path filePath = dataFilePathFactory.toPath(file);
                    FormatReaderContext readerContext =
                            new FormatReaderContext(fileIO, filePath, file.fileSize());
                    try (RecordReader<InternalRow> reader =
                            readerFactory.createReader(readerContext)) {
                        reader.forEachRemaining(
                                row -> {
                                    try {
                                        writer.write(row);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                    }
                }
                writer.close();
            } catch (Throwable t) {
                writer.abort();
                throw t;
            }

            DataFileMeta result = writer.result();
            result = result.assignFirstRowId(firstRowId);
            result = result.assignSequenceNumber(minSeq, maxSeq);
            compactAfter.add(result);
        }

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        compactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        return new CommitMessageImpl(
                partition, 0, null, DataIncrement.emptyIncrement(), compactIncrement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, compactBefore, compactAfter, blobTask);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataEvolutionCompactTask that = (DataEvolutionCompactTask) o;
        return blobTask == that.blobTask
                && Objects.equals(partition, that.partition)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    @Override
    public String toString() {
        return String.format(
                "DataEvolutionCompactTask {"
                        + "partition = %s, "
                        + "compactBefore = %s, "
                        + "compactAfter = %s, "
                        + "blobTask = %s}",
                partition, compactBefore, compactAfter, blobTask);
    }
}
