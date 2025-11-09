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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.UriReaderFactory;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test write and read table with blob type. */
public class BlobTableITCase extends CatalogITCaseBase {

    private static final Random RANDOM = new Random();
    @TempDir private Path warehouse;

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS blob_table (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')",
                "CREATE TABLE IF NOT EXISTS blob_table_descriptor (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture', 'blob-as-descriptor'='true')");
    }

    @Test
    public void testBasic() {
        batchSql("SELECT * FROM blob_table");
        batchSql("INSERT INTO blob_table VALUES (1, 'paimon', X'48656C6C6F')");
        assertThat(batchSql("SELECT * FROM blob_table"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "paimon", new byte[] {72, 101, 108, 108, 111}));
        assertThat(batchSql("SELECT picture FROM blob_table"))
                .containsExactlyInAnyOrder(Row.of(new byte[] {72, 101, 108, 108, 111}));
        assertThat(batchSql("SELECT file_path FROM `blob_table$files`").size()).isEqualTo(2);
    }

    @Test
    public void testWriteBlobAsDescriptor() throws Exception {
        byte[] blobData = new byte[1024 * 1024];
        RANDOM.nextBytes(blobData);
        FileIO fileIO = new LocalFileIO();
        String uri = "file://" + warehouse + "/external_blob";
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(uri), true)) {
            outputStream.write(blobData);
        }

        BlobDescriptor blobDescriptor = new BlobDescriptor(uri, 0, blobData.length);
        batchSql(
                "INSERT INTO blob_table_descriptor VALUES (1, 'paimon', X'"
                        + bytesToHex(blobDescriptor.serialize())
                        + "')");
        byte[] newDescriptorBytes =
                (byte[]) batchSql("SELECT picture FROM blob_table_descriptor").get(0).getField(0);
        BlobDescriptor newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes);
        Options options = new Options();
        options.set("warehouse", warehouse.toString());
        CatalogContext catalogContext = CatalogContext.create(options);
        UriReaderFactory uriReaderFactory = new UriReaderFactory(catalogContext);
        Blob blob =
                Blob.fromDescriptor(
                        uriReaderFactory.create(newBlobDescriptor.uri()), blobDescriptor);
        assertThat(blob.toData()).isEqualTo(blobData);
        batchSql("ALTER TABLE blob_table_descriptor SET ('blob-as-descriptor'='false')");
        assertThat(batchSql("SELECT * FROM blob_table_descriptor"))
                .containsExactlyInAnyOrder(Row.of(1, "paimon", blobData));
    }

    @Test
    public void testBlobDescriptorUriSchemePreservation() throws Exception {
        // This test verifies that blob descriptor URI scheme is preserved after write/read
        // in blob-as-descriptor=true mode.
        //
        // This test reproduces the issue where:
        // - blob-as-descriptor=true mode
        // - User provides URI WITH scheme (e.g., 'pangu://pangu/path/to/file' or
        // 'file:///path/to/file')
        // - After writing and reading, BlobDescriptor.uri should preserve the scheme
        //
        // Expected behavior:
        // - BlobDescriptor.uri should preserve the scheme after serialize/deserialize
        // - The URI should maintain its original format with scheme prefix

        // Create a test blob file in temp directory (simulating external storage)
        byte[] testBlobData = "test blob data for URI scheme preservation".getBytes();
        String externalBlobPath = warehouse + "/external_blob_with_scheme";
        FileIO fileIO = new LocalFileIO();
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(externalBlobPath), true)) {
            outputStream.write(testBlobData);
        }

        // Create blob descriptor with URI WITH scheme (simulating Pangu path with scheme)
        // User provides: 'file:///path/to/file' (WITH scheme)
        // In real Pangu scenarios, it would be: 'pangu://pangu/volume/path/to/file'
        String originalUriWithScheme = "file://" + externalBlobPath; // URI WITH scheme
        BlobDescriptor blobDescriptor =
                new BlobDescriptor(originalUriWithScheme, 0, testBlobData.length);

        // Verify the original URI has scheme
        assertThat(originalUriWithScheme)
                .as("Test setup: Original URI should have scheme")
                .startsWith("file://");

        // Write data with blob descriptor (URI WITH scheme)
        batchSql(
                "INSERT INTO blob_table_descriptor VALUES (1, 'test', X'"
                        + bytesToHex(blobDescriptor.serialize())
                        + "')");

        // Read data back
        List<Row> result = batchSql("SELECT * FROM blob_table_descriptor");
        assertThat(result).hasSize(1);

        Row row = result.get(0);
        assertThat(row.getField(0)).isEqualTo(1);
        assertThat(row.getField(1)).isEqualTo("test");

        // Get the blob descriptor bytes from the result
        byte[] newDescriptorBytes = (byte[]) row.getField(2);
        assertThat(newDescriptorBytes).isNotNull();

        // Deserialize the blob descriptor
        BlobDescriptor newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes);

        // THIS IS WHERE THE ISSUE OCCURS (if it exists):
        // After from_descriptor (reading from table), the URI should still have the scheme
        // But it might have lost the scheme during the write/read process
        String descriptorUriAfterRead = newBlobDescriptor.uri();

        // Check if URI still has scheme
        boolean hasSchemeAfterRead =
                descriptorUriAfterRead.startsWith("file://")
                        || descriptorUriAfterRead.startsWith("pangu://")
                        || descriptorUriAfterRead.startsWith("http://")
                        || descriptorUriAfterRead.startsWith("https://");

        // Verify the scheme is preserved
        assertThat(hasSchemeAfterRead)
                .as(
                        "URI scheme should be preserved after write/read. "
                                + "Original URI: %s, After read URI: %s. "
                                + "This reproduces the issue where Pangu paths lose their scheme prefix "
                                + "after being written and read from the table.",
                        originalUriWithScheme, descriptorUriAfterRead)
                .isTrue();

        // If scheme is preserved, try to create reader and read the blob
        Options options = new Options();
        options.set("warehouse", warehouse.toString());
        CatalogContext catalogContext = CatalogContext.create(options);
        UriReaderFactory uriReaderFactory = new UriReaderFactory(catalogContext);
        Blob blob =
                Blob.fromDescriptor(
                        uriReaderFactory.create(newBlobDescriptor.uri()), newBlobDescriptor);
        assertThat(blob.toData()).isEqualTo(testBlobData);
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
