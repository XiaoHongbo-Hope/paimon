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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for schema changes. */
public class SchemaChangeITCase extends CatalogITCaseBase {

    // TODO cover more cases.
    @Test
    public void testAddColumn() {
        sql("CREATE TABLE T (a STRING, b DOUBLE, c FLOAT)");
        sql("INSERT INTO T VALUES('aaa', 1.2, 3.4)");
        sql("ALTER TABLE T ADD d INT");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647),\n"
                                + "  `b` DOUBLE,\n"
                                + "  `c` FLOAT,\n"
                                + "  `d` INT\n"
                                + ")");
        sql("INSERT INTO T VALUES('bbb', 4.5, 5.6, 5)");
        result = sql("SELECT * FROM T");
        assertThat(result.toString()).isEqualTo("[+I[aaa, 1.2, 3.4, null], +I[bbb, 4.5, 5.6, 5]]");

        // add column with after position
        sql("ALTER TABLE T ADD e INT AFTER b");
        result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647),\n"
                                + "  `b` DOUBLE,\n"
                                + "  `e` INT,\n"
                                + "  `c` FLOAT,\n"
                                + "  `d` INT");
        sql("INSERT INTO T VALUES('ccc', 2.3, 6, 5.6, 5)");
        result = sql("SELECT * FROM T");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[aaa, 1.2, null, 3.4, null], +I[bbb, 4.5, null, 5.6, 5], +I[ccc, 2.3, 6, 5.6, 5]]");

        // add column with first position
        sql("ALTER TABLE T ADD f STRING FIRST");
        result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `f` VARCHAR(2147483647),\n"
                                + "  `a` VARCHAR(2147483647),\n"
                                + "  `b` DOUBLE,\n"
                                + "  `e` INT,\n"
                                + "  `c` FLOAT,\n"
                                + "  `d` INT");

        sql("INSERT INTO T VALUES('flink', 'fff', 45.34, 4, 2.45, 12)");
        result = sql("SELECT * FROM T");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[null, aaa, 1.2, null, 3.4, null], +I[null, bbb, 4.5, null, 5.6, 5],"
                                + " +I[null, ccc, 2.3, 6, 5.6, 5], +I[flink, fff, 45.34, 4, 2.45, 12]]");

        // add multiple columns.
        sql("ALTER TABLE T ADD ( g INT, h BOOLEAN ) ");
        sql("INSERT INTO T VALUES('ggg', 'hhh', 23.43, 6, 2.34, 34, 23, true)");
        result = sql("SELECT * FROM T");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[null, aaa, 1.2, null, 3.4, null, null, null], +I[null, bbb, 4.5, null, 5.6, 5, null, null],"
                                + " +I[null, ccc, 2.3, 6, 5.6, 5, null, null], +I[flink, fff, 45.34, 4, 2.45, 12, null, null],"
                                + " +I[ggg, hhh, 23.43, 6, 2.34, 34, 23, true]]");
    }

    @Test
    public void testDropColumn() {
        sql(
                "CREATE TABLE T (a STRING PRIMARY KEY NOT ENFORCED, b STRING, c STRING, d INT, e FLOAT)");
        sql("INSERT INTO T VALUES('aaa', 'bbb', 'ccc', 10, 3.4)");
        sql("ALTER TABLE T DROP e");
        sql("INSERT INTO T VALUES('ddd', 'eee', 'fff', 20)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `c` VARCHAR(2147483647),\n"
                                + "  `d` INT,");
        result = sql("SELECT * FROM T");
        assertThat(result.toString()).isEqualTo("[+I[aaa, bbb, ccc, 10], +I[ddd, eee, fff, 20]]");

        sql("ALTER TABLE T DROP (c, d)");
        result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),");

        sql("INSERT INTO T VALUES('ggg', 'hhh')");
        result = sql("SELECT * FROM T");
        assertThat(result.toString()).isEqualTo("[+I[aaa, bbb], +I[ddd, eee], +I[ggg, hhh]]");
    }

    @Test
    public void testRenameColumn() {
        sql("CREATE TABLE T (a STRING PRIMARY KEY NOT ENFORCED, b STRING, c STRING)");
        sql("INSERT INTO T VALUES('paimon', 'bbb', 'ccc')");
        sql("ALTER TABLE T RENAME c TO c1");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `c1` VARCHAR(2147483647)");
        result = sql("SELECT a, b, c1 FROM T");
        assertThat(result.toString()).isEqualTo("[+I[paimon, bbb, ccc]]");

        // column do not exist.
        assertThatThrownBy(() -> sql("ALTER TABLE T RENAME d TO d1"))
                .hasMessageContaining("The column `d` does not exist in the base table.");

        // target column exist.
        assertThatThrownBy(() -> sql("ALTER TABLE T RENAME a TO b"))
                .hasMessageContaining("The column `b` already existed in table schema.");
    }

    @Test
    public void testDropPrimaryKey() {
        sql("CREATE TABLE T (a STRING PRIMARY KEY NOT ENFORCED, b STRING, c STRING)");
        assertThatThrownBy(() -> sql("ALTER TABLE T DROP a"))
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "The column `a` is used as the primary key.");
    }

    @Test
    public void testDropPartitionKey() {
        sql(
                "CREATE TABLE MyTable (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING,\n"
                        + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                        + ") PARTITIONED BY (dt, hh)");
        assertThatThrownBy(() -> sql("ALTER TABLE MyTable DROP dt"))
                .hasMessageContaining(
                        "Failed to execute ALTER TABLE statement.\n"
                                + "The column `dt` is used as the partition keys.");
    }

    @Test
    public void testModifyColumnTypeFromNumericToNumericPrimitive() {
        // decimal and numeric primitive to numeric primitive
        sql(
                "CREATE TABLE T (a TINYINT COMMENT 'a field', b INT COMMENT 'b field', c FLOAT COMMENT 'c field', d DOUBLE, e DECIMAL(10, 4), f DECIMAL(10, 4), g DOUBLE)");
        sql(
                "INSERT INTO T VALUES(cast(1 as TINYINT), 123, 1.23, 3.141592, 3.14156, 3.14159, 1.23)");

        sql(
                "ALTER TABLE T MODIFY (a INT, b SMALLINT, c DOUBLE, d FLOAT, e BIGINT, f DOUBLE, g TINYINT)");

        List<String> result =
                sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, INT, true, null, null, null, a field]",
                        "+I[b, SMALLINT, true, null, null, null, b field]",
                        "+I[c, DOUBLE, true, null, null, null, c field]",
                        "+I[d, FLOAT, true, null, null, null, null]",
                        "+I[e, BIGINT, true, null, null, null, null]",
                        "+I[f, DOUBLE, true, null, null, null, null]",
                        "+I[g, TINYINT, true, null, null, null, null]");

        sql(
                "INSERT INTO T VALUES(2, cast(456 as SMALLINT), 4.56, 3.14, 456, 4.56, cast(2 as TINYINT))");
        result =
                sql("SELECT * FROM T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[1, 123, 1.2300000190734863, 3.141592, 3, 3.1416, 1]",
                        "+I[2, 456, 4.56, 3.14, 456, 4.56, 2]");
    }

    @Test
    public void testModifyColumnTypeFromNumericToDecimal() {
        // decimal and numeric primitive to decimal
        sql("CREATE TABLE T (a DECIMAL(10, 4), b DECIMAL(10, 2), c INT, d FLOAT)");
        sql("INSERT INTO T VALUES(1.23456, 1.23, 123, 3.14156)");

        sql(
                "ALTER TABLE T MODIFY (a DECIMAL(10, 2), b DECIMAL(10, 4), c DECIMAL(10, 4), d DECIMAL(10, 4))");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` DECIMAL(10, 2),\n"
                                + "  `b` DECIMAL(10, 4),\n"
                                + "  `c` DECIMAL(10, 4),\n"
                                + "  `d` DECIMAL(10, 4)");
        sql("INSERT INTO T VALUES(1.2, 1.2345, 456, 4.13)");
        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[1.20, 1.2345, 456.0000, 4.1300]", "+I[1.23, 1.2300, 123.0000, 3.1416]");
    }

    @Test
    public void testModifyColumnTypeBooleanAndNumeric() {
        // boolean To numeric and numeric To boolean
        sql("CREATE TABLE T (a BOOLEAN, b BOOLEAN, c TINYINT, d INT, e BIGINT, f DOUBLE)");
        sql(
                "INSERT INTO T VALUES(true, false, cast(0 as TINYINT), 1 , -9223372036854775808, 3.14)");

        sql("ALTER TABLE T MODIFY (a TINYINT, b INT, c BOOLEAN, d BOOLEAN, e BOOLEAN)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` TINYINT,\n"
                                + "  `b` INT,\n"
                                + "  `c` BOOLEAN,\n"
                                + "  `d` BOOLEAN,\n"
                                + "  `e` BOOLEAN,");
        sql("INSERT INTO T VALUES(cast(1 as TINYINT), 123, true, true, false, 4.13)");
        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[1, 123, true, true, false, 4.13]", "+I[1, 0, false, true, true, 3.14]");

        assertThatThrownBy(() -> sql("ALTER TABLE T MODIFY (f BOOLEAN)"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage(
                        "Column type f[DOUBLE] cannot be converted to BOOLEAN without loosing information.");
    }

    @Test
    public void testModifyColumnTypeFromNumericToString() {
        sql(
                "CREATE TABLE T (a STRING PRIMARY KEY NOT ENFORCED, b INT, c DECIMAL(10, 3), d FLOAT, e DOUBLE)");
        sql("INSERT INTO T VALUES('paimon', 123, 300.123, 400.123, 400.1234)");

        sql("ALTER TABLE T MODIFY (b STRING, c VARCHAR(6), d CHAR(3), e CHAR(10))");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `c` VARCHAR(6),\n"
                                + "  `d` CHAR(3),\n"
                                + "  `e` CHAR(10),");
        sql("INSERT INTO T VALUES('apache', '345', '200', '0.12', '1000.12345')");
        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[apache, 345, 200, 0.1, 1000.12345]",
                        "+I[paimon, 123, 300.12, 400, 400.1234  ]");
    }

    @Test
    public void testModifyColumnTypeFromBooleanToString() {
        sql("CREATE TABLE T (a STRING PRIMARY KEY NOT ENFORCED, b BOOLEAN, c BOOLEAN)");
        sql("INSERT INTO T VALUES('paimon', true, false)");

        sql("ALTER TABLE T MODIFY (b STRING, c STRING)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `c` VARCHAR(2147483647),");
        sql("INSERT INTO T VALUES('apache', '345', '200')");
        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("+I[apache, 345, 200]", "+I[paimon, true, false]");
    }

    @Test
    public void testModifyColumnTypeFromTimestampToString() {
        // timestamp/date/time/timestamp_ltz to string
        sql(
                "CREATE TABLE T (a STRING PRIMARY KEY NOT ENFORCED, b TIMESTAMP(3), c TIMESTAMP(6), d DATE, f TIME, g TIMESTAMP(3) WITH LOCAL TIME ZONE)");
        sql(
                "INSERT INTO T VALUES('paimon', TIMESTAMP '2023-06-06 12:00:00', TIMESTAMP '2023-06-06 08:00:00.123456', DATE '2023-05-31', TIME '14:30:00', TO_TIMESTAMP_LTZ(4001, 3))");

        sql("ALTER TABLE T MODIFY (b STRING, c STRING, d STRING, f STRING, g STRING)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `c` VARCHAR(2147483647),\n"
                                + "  `d` VARCHAR(2147483647),\n"
                                + "  `f` VARCHAR(2147483647),\n"
                                + "  `g` VARCHAR(2147483647),");
        sql(
                "INSERT INTO T VALUES('apache', '2023-06-07 12:00:00', '2023-06-07 08:00:00.123456', '2023-06-07', '08:00:00', '2023-06-07 00:00:00.123456')");
        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[apache, 2023-06-07 12:00:00, 2023-06-07 08:00:00.123456, 2023-06-07, 08:00:00, 2023-06-07 00:00:00.123456]",
                        "+I[paimon, 2023-06-06 12:00:00.000, 2023-06-06 08:00:00.123456, 2023-05-31, 14:30:00, "
                                + BinaryString.fromString(
                                        DateTimeUtils.formatTimestamp(
                                                DateTimeUtils.parseTimestampData(
                                                        "1970-01-01 00:00:04.001", 3),
                                                TimeZone.getDefault(),
                                                3))
                                + "]");
    }

    @Test
    public void testModifyColumnTypeFromStringToString() {
        sql("CREATE TABLE T (b VARCHAR(10), c VARCHAR(10), d CHAR(5), e CHAR(5))");
        sql("INSERT INTO T VALUES('paimon', '1234567890', '12345', '12345')");

        sql("ALTER TABLE T MODIFY (b VARCHAR(5), c CHAR(5), d VARCHAR(5), e CHAR(6))");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `b` VARCHAR(5),\n"
                                + "  `c` CHAR(5),\n"
                                + "  `d` VARCHAR(5),\n"
                                + "  `e` CHAR(6)");
        sql("INSERT INTO T VALUES('apache', '1234567890', '123456', '1234567')");
        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[apach, 12345, 12345, 123456]", "+I[paimo, 12345, 12345, 12345 ]");
    }

    @Test
    public void testModifyColumnTypeFromStringToBoolean() {
        sql("CREATE TABLE T (b VARCHAR(10), c VARCHAR(10), d STRING, e CHAR(1))");
        sql("INSERT INTO T VALUES('true', '1', 'yes', 'y')");
        sql("INSERT INTO T VALUES('false', '0', 'no', 'n')");

        sql("ALTER TABLE T MODIFY (b BOOLEAN, c BOOLEAN, d BOOLEAN, e BOOLEAN)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `b` BOOLEAN,\n"
                                + "  `c` BOOLEAN,\n"
                                + "  `d` BOOLEAN,\n"
                                + "  `e` BOOLEAN");
        sql("INSERT INTO T VALUES(false, true, false, true)");
        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[true, true, true, true]",
                        "+I[false, false, false, false]",
                        "+I[false, true, false, true]");
    }

    @Test
    public void testModifyColumnTypeFromStringToNumeric() {
        // string to decimal/numeric primitive
        sql("CREATE TABLE T (a VARCHAR(10), b CHAR(1), c VARCHAR(10), d STRING, e STRING)");
        sql("INSERT INTO T VALUES('3.14', '1', '123', '3.14', '3.14')");

        sql("ALTER TABLE T MODIFY (a DECIMAL(5, 4), b TINYINT, c INT, d DOUBLE, e BIGINT)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` DECIMAL(5, 4),\n"
                                + "  `b` TINYINT,\n"
                                + "  `c` INT,\n"
                                + "  `d` DOUBLE,\n"
                                + "  `e` BIGINT");
        sql("INSERT INTO T VALUES(4.13, cast(2 as TINYINT), 456, 3.14, 4)");
        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[3.1400, 1, 123, 3.14, 3]", "+I[4.1300, 2, 456, 3.14, 4]");

        sql("CREATE TABLE T1 (a STRING, b STRING)");
        sql("ALTER TABLE T1 SET ('disable-explicit-type-casting' = 'false')");
        sql("INSERT INTO T1 VALUES('test', '3.14')");

        sql("ALTER TABLE T1 MODIFY (a INT, b TINYINT)");
        assertThatThrownBy(() -> sql("SELECT * FROM T1"))
                .hasRootCauseInstanceOf(NumberFormatException.class)
                .hasRootCauseMessage("For input string: 'test'. Invalid character found.");
    }

    @Test
    public void testModifyColumnTypeFromStringToTimestamp() {
        // string to timestamp/date/time/timestamp_ltz
        sql("CREATE TABLE T (a VARCHAR(30), b CHAR(20), c VARCHAR(20), d STRING, e STRING)");
        sql(
                "INSERT INTO T VALUES('2022-12-12 09:30:10', '2022-12-12', '09:30:00', '2022-12-12 09:30:00.123456', '2022-12-12 00:30:00.123456')");

        sql(
                "ALTER TABLE T MODIFY (a TIMESTAMP, b DATE, c TIME, d TIMESTAMP(3), e TIMESTAMP(3) WITH LOCAL TIME ZONE)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` TIMESTAMP(6),\n"
                                + "  `b` DATE,\n"
                                + "  `c` TIME(0),\n"
                                + "  `d` TIMESTAMP(3),\n"
                                + "  `e` TIMESTAMP(3) WITH LOCAL TIME ZONE");

        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[2022-12-12T09:30:10, 2022-12-12, 09:30, 2022-12-12T09:30:00.123, "
                                + DateTimeUtils.timestampToTimestampWithLocalZone(
                                                DateTimeUtils.parseTimestampData(
                                                        "2022-12-12 00:30:00.123456", 3),
                                                TimeZone.getDefault())
                                        .toLocalDateTime()
                                        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                                + "Z"
                                + "]");
    }

    @Test
    public void testModifyColumnTypeStringToBinary() {
        sql("CREATE TABLE T (a VARCHAR(5), b VARCHAR(10), c VARCHAR(10), d VARCHAR(10))");
        sql(
                "INSERT INTO T VALUES('Apache Paimon', 'Apache Paimon','Apache Paimon','Apache Paimon')");

        sql("ALTER TABLE T MODIFY (a BINARY(10), b BINARY(5), c VARBINARY(5), d VARBINARY(20))");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` BINARY(10),\n"
                                + "  `b` BINARY(5),\n"
                                + "  `c` VARBINARY(5),\n"
                                + "  `d` VARBINARY(20)");

        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[[65, 112, 97, 99, 104, 0, 0, 0, 0, 0], [65, 112, 97, 99, 104], [65, 112, 97, 99, 104], [65, 112, 97, 99, 104, 101, 32, 80, 97, 105]]");
    }

    @Test
    public void testModifyColumnTypeFromTimestampToTimestamp() {
        // timestamp/timestamp_ltz to timestamp/timestamp_ltz
        sql(
                "CREATE TABLE T (a TIMESTAMP(6), b TIMESTAMP(6), c TIMESTAMP(6) WITH LOCAL TIME ZONE, d TIMESTAMP(6) WITH LOCAL TIME ZONE)");
        sql(
                "INSERT INTO T VALUES(TIMESTAMP '2022-12-01 09:00:00.123456', TIMESTAMP '2022-12-02 09:00:00.123456', TO_TIMESTAMP_LTZ(4001, 3), TO_TIMESTAMP_LTZ(4001, 3))");

        sql(
                "ALTER TABLE T MODIFY (a TIMESTAMP(3), b TIMESTAMP(6) WITH LOCAL TIME ZONE, c TIMESTAMP(3) WITH LOCAL TIME ZONE, d TIMESTAMP(3))");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` TIMESTAMP(3),\n"
                                + "  `b` TIMESTAMP(6) WITH LOCAL TIME ZONE,\n"
                                + "  `c` TIMESTAMP(3) WITH LOCAL TIME ZONE,\n"
                                + "  `d` TIMESTAMP(3)");

        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[2022-12-01T09:00:00.123, "
                                + DateTimeUtils.timestampToTimestampWithLocalZone(
                                                DateTimeUtils.parseTimestampData(
                                                        "2022-12-02 09:00:00.123456", 6),
                                                TimeZone.getDefault())
                                        .toLocalDateTime()
                                        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                                + "Z, "
                                + "1970-01-01T00:00:04.001Z, "
                                + DateTimeUtils.timestampWithLocalZoneToTimestamp(
                                                DateTimeUtils.parseTimestampData(
                                                        "1970-01-01 00:00:04.001", 3),
                                                TimeZone.getDefault())
                                        .toLocalDateTime()
                                        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                                + "]");
    }

    @Test
    public void testModifyColumnTypeFromDateToTimestamp() {
        // date to timestamp/timestamp_ltz
        sql("CREATE TABLE T (a DATE, b DATE)");
        sql("INSERT INTO T VALUES(DATE '2022-12-12', DATE '2022-12-11')");

        sql("ALTER TABLE T MODIFY (a TIMESTAMP(6), b TIMESTAMP(6) WITH LOCAL TIME ZONE)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` TIMESTAMP(6),\n"
                                + "  `b` TIMESTAMP(6) WITH LOCAL TIME ZONE");
        result = sql("SELECT * FROM T");

        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[2022-12-12T00:00, "
                                + DateTimeUtils.timestampToTimestampWithLocalZone(
                                                DateTimeUtils.parseTimestampData("2022-12-11", 6),
                                                TimeZone.getDefault())
                                        .toLocalDateTime()
                                        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                                + "Z"
                                + "]");
    }

    @Test
    public void testModifyColumnTypeFromTimeToTimestamp() {
        // time to timestamp/timestamp_ltz
        sql("CREATE TABLE T (a TIME, b TIME(2), c TIME(3))");
        sql("INSERT INTO T VALUES(TIME '09:30:10', TIME '09:30:10.24', TIME '09:30:10.123')");

        sql(
                "ALTER TABLE T MODIFY (a TIMESTAMP(3), b TIMESTAMP(6), c TIMESTAMP(6) WITH LOCAL TIME ZONE)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` TIMESTAMP(3),\n"
                                + "  `b` TIMESTAMP(6),\n"
                                + "  `c` TIMESTAMP(6) WITH LOCAL TIME ZONE");

        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[1970-01-01T09:30:10, 1970-01-01T09:30:10.240, 1970-01-01T09:30:10.123Z]");
    }

    @Test
    public void testModifyColumnTypeBinaryToBinary() {
        sql(
                "CREATE TABLE T (a BINARY(5), b BINARY(10), c BINARY(10), d BINARY(10), e VARBINARY(5), f VARBINARY(10), g VARBINARY(10), h VARBINARY(10))");
        sql(
                "INSERT INTO T VALUES(X'0123456789', X'0123456789',X'0123456789',X'0123456789',X'0123456789',X'0123456789',X'0123456789',X'0123456789')");

        sql(
                "ALTER TABLE T MODIFY (a BINARY(10), b BINARY(5), c VARBINARY(5), d VARBINARY(20), e VARBINARY(10), f VARBINARY(5), g BINARY(5), h BINARY(20))");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` BINARY(10),\n"
                                + "  `b` BINARY(5),\n"
                                + "  `c` VARBINARY(5),\n"
                                + "  `d` VARBINARY(20),\n"
                                + "  `e` VARBINARY(10),\n"
                                + "  `f` VARBINARY(5),\n"
                                + "  `g` BINARY(5),\n"
                                + "  `h` BINARY(20)");

        result = sql("SELECT * FROM T");
        assertThat(result.stream().map(Objects::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[[1, 35, 69, 103, -119, 0, 0, 0, 0, 0], [1, 35, 69, 103, -119], [1, 35, 69, 103, -119], [1, 35, 69, 103, -119, 0, 0, 0, 0, 0], [1, 35, 69, 103, -119], [1, 35, 69, 103, -119], [1, 35, 69, 103, -119], [1, 35, 69, 103, -119, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]");
    }

    @Test
    public void testModifyColumnPosition() {
        sql(
                "CREATE TABLE T (a STRING PRIMARY KEY NOT ENFORCED, b STRING, c STRING, d INT, e DOUBLE)");
        sql("INSERT INTO T VALUES('paimon', 'bbb', 'ccc', 1, 3.4)");
        sql("ALTER TABLE T MODIFY b STRING FIRST");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `c` VARCHAR(2147483647),\n"
                                + "  `d` INT,\n"
                                + "  `e` DOUBLE,");

        sql("INSERT INTO T VALUES('aaa', 'flink', 'ddd', 2, 5.7)");
        result = sql("SELECT * FROM T");
        assertThat(result.toString())
                .isEqualTo("[+I[aaa, flink, ddd, 2, 5.7], +I[bbb, paimon, ccc, 1, 3.4]]");

        sql("ALTER TABLE T MODIFY e DOUBLE AFTER c");
        result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `c` VARCHAR(2147483647),\n"
                                + "  `e` DOUBLE,\n"
                                + "  `d` INT,");

        sql("INSERT INTO T VALUES('sss', 'ggg', 'eee', 4.7, 10)");
        result = sql("SELECT * FROM T");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[aaa, flink, ddd, 5.7, 2], +I[sss, ggg, eee, 4.7, 10], +I[bbb, paimon, ccc, 3.4, 1]]");

        //  move self to first test
        assertThatThrownBy(() -> sql("ALTER TABLE T MODIFY b STRING FIRST"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot move itself for column b"));

        //  move self to after test
        assertThatThrownBy(() -> sql("ALTER TABLE T MODIFY b STRING AFTER b"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot move itself for column b"));

        // missing column
        assertThatThrownBy(() -> sql("ALTER TABLE T MODIFY h STRING FIRST"))
                .hasMessageContaining(
                        "Try to modify a column `h` which does not exist in the table");

        assertThatThrownBy(() -> sql("ALTER TABLE T MODIFY h STRING AFTER d"))
                .hasMessageContaining(
                        "Try to modify a column `h` which does not exist in the table");
    }

    @Test
    public void testModifyNullability() {
        sql(
                "CREATE TABLE T (a STRING PRIMARY KEY NOT ENFORCED, b STRING, c STRING, d INT, e FLOAT NOT NULL)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `c` VARCHAR(2147483647),\n"
                                + "  `d` INT,\n"
                                + "  `e` FLOAT NOT NULL,");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "INSERT INTO T VALUES('aaa', 'bbb', 'ccc', 1, CAST(NULL AS FLOAT))"))
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "Column 'e' is NOT NULL, however, a null value is being written into it."));

        // Not null -> nullable
        sql("ALTER TABLE T MODIFY e FLOAT");
        result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `c` VARCHAR(2147483647),\n"
                                + "  `d` INT,\n"
                                + "  `e` FLOAT");

        // Nullable -> not null
        sql("ALTER TABLE T SET ('alter-column-null-to-not-null.disabled' = 'false')");
        sql("ALTER TABLE T MODIFY c STRING NOT NULL");
        result = sql("SHOW CREATE TABLE T");
        assertThat(result.toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `b` VARCHAR(2147483647),\n"
                                + "  `c` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `d` INT,\n"
                                + "  `e` FLOAT");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "INSERT INTO T VALUES('aaa', 'bbb', CAST(NULL AS STRING), 1, CAST(NULL AS FLOAT))"))
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "Column 'c' is NOT NULL, however, a null value is being written into it."));

        // Insert a null value
        sql("INSERT INTO T VALUES('aaa', 'bbb', 'ccc', 1, CAST(NULL AS FLOAT))");
        result = sql("select * from T");
        assertThat(result.toString()).isEqualTo("[+I[aaa, bbb, ccc, 1, null]]");

        // Then nullable -> not null
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER,
                        ExecutionConfigOptions.NotNullEnforcer.DROP);
        sql("ALTER TABLE T MODIFY e FLOAT NOT NULL;\n");
        sql("INSERT INTO T VALUES('aa2', 'bb2', 'cc2', 2, 2.5)");
        result = sql("select * from T");
        assertThat(result.toString()).isEqualTo("[+I[aa2, bb2, cc2, 2, 2.5]]");
    }

    @Test
    public void testModifyColumnComment() {
        sql("CREATE TABLE T (a STRING, b STRING COMMENT 'from column b')");
        List<String> result =
                sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, null]",
                        "+I[b, STRING, true, null, null, null, from column b]");

        // add column comment
        sql("ALTER TABLE T MODIFY a STRING COMMENT 'from column a'");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, from column a]",
                        "+I[b, STRING, true, null, null, null, from column b]");

        // update column comment
        sql("ALTER TABLE T MODIFY b STRING COMMENT 'from column b updated'");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, from column a]",
                        "+I[b, STRING, true, null, null, null, from column b updated]");
    }

    @Test
    public void testAddWatermark() {
        sql("CREATE TABLE T (a STRING, ts TIMESTAMP(3))");
        List<String> result =
                sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null]",
                        "+I[ts, TIMESTAMP(3), true, null, null, null]");

        // add watermark
        sql("ALTER TABLE T ADD WATERMARK FOR ts AS ts - INTERVAL '1' HOUR");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null]",
                        "+I[ts, TIMESTAMP(3), true, null, null, `ts` - INTERVAL '1' HOUR]");

        // add one more watermark
        assertThatThrownBy(
                        () -> sql("ALTER TABLE T ADD WATERMARK FOR ts AS ts - INTERVAL '2' HOUR"))
                .hasMessageContaining("The base table has already defined the watermark strategy");
    }

    @Test
    public void testDropWatermark() {
        sql(
                "CREATE TABLE T (a STRING, ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '1' HOUR)");
        List<String> result =
                sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null]",
                        "+I[ts, TIMESTAMP(3), true, null, null, `ts` - INTERVAL '1' HOUR]");

        // drop watermark
        sql("ALTER TABLE T DROP WATERMARK");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null]",
                        "+I[ts, TIMESTAMP(3), true, null, null, null]");

        // drop again
        assertThatThrownBy(() -> sql("ALTER TABLE T DROP WATERMARK"))
                .hasMessageContaining("The base table does not define any watermark strategy");
    }

    @Test
    public void testModifyWatermark() {
        sql("CREATE TABLE T (a STRING, ts TIMESTAMP(3))");

        // modify watermark
        assertThatThrownBy(
                        () ->
                                sql(
                                        "ALTER TABLE T MODIFY WATERMARK FOR ts AS ts - INTERVAL '1' HOUR"))
                .hasMessageContaining("The base table does not define any watermark");

        // add watermark
        sql("ALTER TABLE T ADD WATERMARK FOR ts AS ts - INTERVAL '1' HOUR");

        // modify watermark
        sql("ALTER TABLE T MODIFY WATERMARK FOR ts AS ts - INTERVAL '2' HOUR");
        List<String> result =
                sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null]",
                        "+I[ts, TIMESTAMP(3), true, null, null, `ts` - INTERVAL '2' HOUR]");
    }

    @Test
    public void testSetAndRemoveOption() throws Exception {
        sql("CREATE TABLE T (a STRING, b STRING, c STRING)");
        sql("ALTER TABLE T SET ('xyc'='unknown1', 'abc'='unknown2')");

        Map<String, String> options = table("T").getOptions();
        assertThat(options).containsEntry("xyc", "unknown1");
        assertThat(options).containsEntry("abc", "unknown2");

        sql("ALTER TABLE T RESET ('xyc', 'abc')");

        options = table("T").getOptions();
        assertThat(options).doesNotContainKey("xyc");
        assertThat(options).doesNotContainKey("abc");
    }

    @Test
    public void testSetAndResetImmutableOptionsOnEmptyTables() {
        sql("CREATE TABLE T1 (a INT, b INT)");
        sql(
                "ALTER TABLE T1 SET ('primary-key' = 'a', 'bucket' = '1', 'merge-engine' = 'first-row')");
        sql("INSERT INTO T1 VALUES (1, 10), (2, 20), (1, 11), (2, 21)");
        assertThat(queryAndSort("SELECT * FROM T1")).containsExactly(Row.of(1, 10), Row.of(2, 20));
        assertThatThrownBy(() -> sql("ALTER TABLE T1 SET ('merge-engine' = 'deduplicate')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'merge-engine' is not supported yet.");

        sql(
                "CREATE TABLE T2 (a INT, b INT, PRIMARY KEY (a) NOT ENFORCED) WITH ('bucket' = '1', 'merge-engine' = 'first-row')");
        sql("ALTER TABLE T2 RESET ('merge-engine')");
        sql("INSERT INTO T2 VALUES (1, 10), (2, 20), (1, 11), (2, 21)");
        assertThat(queryAndSort("SELECT * FROM T2")).containsExactly(Row.of(1, 11), Row.of(2, 21));
    }

    @Test
    public void testSetAndResetImmutableOptionsOnNonEmptyTables() {
        // bucket-key is immutable
        sql(
                "CREATE TABLE T1 (a STRING, b STRING, c STRING) WITH ('bucket' = '1', 'bucket-key' = 'a')");
        sql("INSERT INTO T1 VALUES ('a', 'b', 'c')");

        assertThatThrownBy(() -> sql("ALTER TABLE T1 SET ('bucket-key' = 'c')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'bucket-key' is not supported yet.");

        sql(
                "CREATE TABLE T2 (a STRING, b STRING, c STRING) WITH ('bucket' = '1', 'bucket-key' = 'c')");
        sql("INSERT INTO T2 VALUES ('a', 'b', 'c')");
        assertThatThrownBy(() -> sql("ALTER TABLE T2 RESET ('bucket-key')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'bucket-key' is not supported yet.");

        // merge-engine is immutable
        sql(
                "CREATE TABLE T4 (a STRING, b STRING, c STRING) WITH ('merge-engine' = 'partial-update')");
        sql("INSERT INTO T4 VALUES ('a', 'b', 'c')");
        assertThatThrownBy(() -> sql("ALTER TABLE T4 RESET ('merge-engine')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'merge-engine' is not supported yet.");

        // sequence.field is immutable
        sql("CREATE TABLE T5 (a STRING, b STRING, c STRING) WITH ('sequence.field' = 'b')");
        sql("INSERT INTO T5 VALUES ('a', 'b', 'c')");
        assertThatThrownBy(() -> sql("ALTER TABLE T5 SET ('sequence.field' = 'c')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Change 'sequence.field' is not supported yet.");
    }

    @Test
    public void testAlterTableComment() throws Exception {
        sql("CREATE TABLE T (a STRING, b STRING, c STRING)");

        // add table comment
        sql("ALTER TABLE T SET ('comment'='t comment')");
        String comment = table("T").getComment();
        assertThat(comment).isEqualTo("t comment");

        // update table comment
        sql("ALTER TABLE T SET ('comment'='t comment v2')");
        comment = table("T").getComment();
        assertThat(comment).isEqualTo("t comment v2");

        // remove table comment
        sql("ALTER TABLE T RESET ('comment')");
        comment = table("T").getComment();
        assertThat(comment).isEmpty();
    }

    @Test
    public void testAlterTableSchema() {
        sql("CREATE TABLE T (a STRING, b STRING COMMENT 'from column b')");
        List<String> result =
                sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, null]",
                        "+I[b, STRING, true, null, null, null, from column b]");

        // add columns at different positions
        sql("ALTER TABLE T ADD (c INT AFTER b)");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, null]",
                        "+I[b, STRING, true, null, null, null, from column b]",
                        "+I[c, INT, true, null, null, null, null]");

        sql("ALTER TABLE T ADD (d INT FIRST)");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[d, INT, true, null, null, null, null]",
                        "+I[a, STRING, true, null, null, null, null]",
                        "+I[b, STRING, true, null, null, null, from column b]",
                        "+I[c, INT, true, null, null, null, null]");

        // drop previously added column
        sql("ALTER TABLE T DROP d");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, null]",
                        "+I[b, STRING, true, null, null, null, from column b]",
                        "+I[c, INT, true, null, null, null, null]");

        // change column type
        sql("ALTER TABLE T MODIFY (c BIGINT)");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, null]",
                        "+I[b, STRING, true, null, null, null, from column b]",
                        "+I[c, BIGINT, true, null, null, null, null]");

        // type change: BIGINT to INT
        sql("ALTER TABLE T MODIFY (c INT)");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, null]",
                        "+I[b, STRING, true, null, null, null, from column b]",
                        "+I[c, INT, true, null, null, null, null]");

        // type change: INT to STRING
        sql("ALTER TABLE T MODIFY (c STRING)");
        result = sql("DESC T").stream().map(Objects::toString).collect(Collectors.toList());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        "+I[a, STRING, true, null, null, null, null]",
                        "+I[b, STRING, true, null, null, null, from column b]",
                        "+I[c, STRING, true, null, null, null, null]");
    }

    @Test
    public void testAlterTableNonPhysicalColumn() {
        sql(
                "CREATE TABLE T (a INT,  c ROW < a INT, d INT> METADATA, b INT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts)");
        sql("ALTER TABLE T ADD e VARCHAR METADATA");
        sql("ALTER TABLE T DROP c ");
        sql("ALTER TABLE T RENAME e TO ee");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.get(0).toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` INT,\n"
                                + "  `b` INT,\n"
                                + "  `ts` TIMESTAMP(3),\n"
                                + "  `ee` VARCHAR(2147483647) METADATA,\n"
                                + "  WATERMARK FOR `ts` AS `ts`\n"
                                + ")")
                .doesNotContain("schema");
    }

    @Test
    public void testSequenceFieldSortOrder() {
        // test default condition which get the largest record
        sql(
                "CREATE TABLE T1 (a STRING PRIMARY KEY NOT ENFORCED, b STRING, c STRING) WITH ('sequence.field'='c')");
        sql("INSERT INTO T1 VALUES ('a', 'b', 'l')");
        sql("INSERT INTO T1 VALUES ('a', 'd', 'n')");
        sql("INSERT INTO T1 VALUES ('a', 'e', 'm')");
        assertThat(sql("select * from T1").toString()).isEqualTo("[+I[a, d, n]]");

        // test for get small record
        sql(
                "CREATE TABLE T2 (a STRING PRIMARY KEY NOT ENFORCED, b STRING, c BIGINT) WITH ('sequence.field'='c', 'sequence.field.sort-order'='descending')");
        sql("INSERT INTO T2 VALUES ('a', 'b', 1)");
        sql("INSERT INTO T2 VALUES ('a', 'd', 3)");
        sql("INSERT INTO T2 VALUES ('a', 'e', 2)");
        assertThat(sql("select * from T2").toString()).isEqualTo("[+I[a, b, 1]]");

        // test for get largest record
        sql(
                "CREATE TABLE T3 (a STRING PRIMARY KEY NOT ENFORCED, b STRING, c DOUBLE) WITH ('sequence.field'='c', 'sequence.field.sort-order'='ascending')");
        sql("INSERT INTO T3 VALUES ('a', 'b', 1.0)");
        sql("INSERT INTO T3 VALUES ('a', 'd', 3.0)");
        sql("INSERT INTO T3 VALUES ('a', 'e', 2.0)");
        assertThat(sql("select * from T3").toString()).isEqualTo("[+I[a, d, 3.0]]");
    }

    @Test
    public void testAlterTableMetadataComment() {
        sql("CREATE TABLE T (a INT, name VARCHAR METADATA COMMENT 'header1', b INT)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.get(0).toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` INT,\n"
                                + "  `name` VARCHAR(2147483647) METADATA COMMENT 'header1',\n"
                                + "  `b` INT\n"
                                + ")")
                .doesNotContain("schema");
        sql("ALTER TABLE T MODIFY name VARCHAR METADATA COMMENT 'header2'");
        result = sql("SHOW CREATE TABLE T");
        assertThat(result.get(0).toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` INT,\n"
                                + "  `name` VARCHAR(2147483647) METADATA COMMENT 'header2',\n"
                                + "  `b` INT\n"
                                + ")")
                .doesNotContain("schema");
        // change name from non-physical column to physical column is not allowed
        assertThatThrownBy(() -> sql("ALTER TABLE T MODIFY name VARCHAR COMMENT 'header3'"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Change is not supported: class org.apache.flink.table.catalog.TableChange$ModifyColumn"));
    }

    @Test
    public void testAlterBucket() {
        sql("CREATE TABLE T1 (a INT PRIMARY KEY NOT ENFORCED, b STRING) WITH ('bucket' = '-1')");
        sql("INSERT INTO T1 VALUES (1, '1')");
        assertThatThrownBy(() -> sql("ALTER TABLE T1 RESET ('bucket')"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class, "Cannot reset bucket."));
        assertThatThrownBy(() -> sql("ALTER TABLE T1 SET ('bucket' = '1')"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot change bucket when it is -1."));

        sql("CREATE TABLE T2 (a INT PRIMARY KEY NOT ENFORCED, b STRING) WITH ('bucket' = '1')");
        sql("INSERT INTO T2 VALUES (1, '1')");
        assertThatThrownBy(() -> sql("ALTER TABLE T2 SET ('bucket' = '-1')"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot change bucket to -1."));
    }

    @ParameterizedTest()
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testUpdateNestedColumn(String formatType) {
        sql(
                "CREATE TABLE T "
                        + "( k INT, v ROW(f1 INT, f2 ROW(f1 STRING, f2 INT NOT NULL)), PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( 'bucket' = '1', 'file.format' = '"
                        + formatType
                        + "' )");
        sql(
                "INSERT INTO T VALUES (1, ROW(10, ROW('apple', 100))), (2, ROW(20, ROW('banana', 200)))");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, Row.of(10, Row.of("apple", 100))),
                        Row.of(2, Row.of(20, Row.of("banana", 200))));

        sql("ALTER TABLE T MODIFY (v ROW(f1 BIGINT, f2 ROW(f3 DOUBLE, f2 INT), f3 STRING))");
        sql(
                "INSERT INTO T VALUES "
                        + "(1, ROW(1000000000001, ROW(101.0, 101), 'cat')), "
                        + "(3, ROW(3000000000001, ROW(301.0, CAST(NULL AS INT)), 'dog'))");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, Row.of(1000000000001L, Row.of(101.0, 101), "cat")),
                        Row.of(2, Row.of(20L, Row.of(null, 200), null)),
                        Row.of(3, Row.of(3000000000001L, Row.of(301.0, null), "dog")));

        sql(
                "ALTER TABLE T MODIFY (v ROW(f1 BIGINT, f2 ROW(f3 DOUBLE, f1 STRING, f2 INT), f3 STRING))");
        sql(
                "INSERT INTO T VALUES "
                        + "(1, ROW(1000000000002, ROW(102.0, 'APPLE', 102), 'cat')), "
                        + "(4, ROW(4000000000002, ROW(402.0, 'LEMON', 402), 'tiger'))");
        assertThat(sql("SELECT k, v.f2.f1, v.f3 FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "APPLE", "cat"),
                        Row.of(2, null, null),
                        Row.of(3, null, "dog"),
                        Row.of(4, "LEMON", "tiger"));

        assertThatCode(() -> sql("ALTER TABLE T MODIFY (v ROW(f1 BIGINT, f2 INT, f3 STRING))"))
                .hasRootCauseMessage(
                        "Column v.f2 can only be updated to row type, and cannot be updated to INTEGER type");
    }

    @ParameterizedTest()
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testUpdateRowInArrayAndMap(String formatType) {
        sql(
                "CREATE TABLE T "
                        + "( k INT, v1 ARRAY<ROW(f1 INT, f2 STRING)>, v2 MAP<INT, ROW(f1 STRING, f2 INT)>, PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( 'bucket' = '1', 'file.format' = '"
                        + formatType
                        + "' )");
        sql(
                "INSERT INTO T VALUES "
                        + "(1, ARRAY[ROW(100, 'apple'), ROW(101, 'banana')], MAP[100, ROW('cat', 1000), 101, ROW('dog', 1001)]), "
                        + "(2, ARRAY[ROW(200, 'pear'), ROW(201, 'grape')], MAP[200, ROW('tiger', 2000), 201, ROW('wolf', 2001)])");

        Map<Integer, Row> map1 = new HashMap<>();
        map1.put(100, Row.of("cat", 1000));
        map1.put(101, Row.of("dog", 1001));
        Map<Integer, Row> map2 = new HashMap<>();
        map2.put(200, Row.of("tiger", 2000));
        map2.put(201, Row.of("wolf", 2001));
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, new Row[] {Row.of(100, "apple"), Row.of(101, "banana")}, map1),
                        Row.of(2, new Row[] {Row.of(200, "pear"), Row.of(201, "grape")}, map2));

        sql(
                "ALTER TABLE T MODIFY (v1 ARRAY<ROW(f1 BIGINT, f2 STRING, f3 STRING)>, v2 MAP<INT, ROW(f3 DOUBLE, f2 INT)>)");
        sql(
                "INSERT INTO T VALUES "
                        + "(1, ARRAY[ROW(1000000000000, 'apple', 'A'), ROW(1000000000001, 'banana', 'B')], MAP[100, ROW(1000.0, 1000), 101, ROW(1001.0, 1001)]), "
                        + "(3, ARRAY[ROW(3000000000000, 'mango', 'M'), ROW(3000000000001, 'cherry', 'C')], MAP[300, ROW(3000.0, 3000), 301, ROW(3001.0, 3001)])");

        map1.clear();
        map1.put(100, Row.of(1000.0, 1000));
        map1.put(101, Row.of(1001.0, 1001));
        map2.clear();
        map2.put(200, Row.of(null, 2000));
        map2.put(201, Row.of(null, 2001));
        Map<Integer, Row> map3 = new HashMap<>();
        map3.put(300, Row.of(3000.0, 3000));
        map3.put(301, Row.of(3001.0, 3001));
        assertThat(sql("SELECT v2, v1, k FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                map1,
                                new Row[] {
                                    Row.of(1000000000000L, "apple", "A"),
                                    Row.of(1000000000001L, "banana", "B")
                                },
                                1),
                        Row.of(
                                map2,
                                new Row[] {Row.of(200L, "pear", null), Row.of(201L, "grape", null)},
                                2),
                        Row.of(
                                map3,
                                new Row[] {
                                    Row.of(3000000000000L, "mango", "M"),
                                    Row.of(3000000000001L, "cherry", "C")
                                },
                                3));
    }

    @ParameterizedTest()
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testUpdateNullabilityPrimitiveType(String formatType) {
        sql(
                "CREATE TABLE T "
                        + "( k INT, v INT NOT NULL, PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( 'bucket' = '1', 'file.format' = '"
                        + formatType
                        + "' )");
        sql("INSERT INTO T VALUES (1, 100), (2, 200)");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 100), Row.of(2, 200));

        sql("ALTER TABLE T MODIFY v INT"); // convert non nullable to nullable
        sql("INSERT INTO T VALUES " + "(3, CAST(NULL AS INT))");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 100), Row.of(2, 200), Row.of(3, null));

        assertThatCode(() -> sql("ALTER TABLE T MODIFY v INT NOT NULL"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v");
    }

    @ParameterizedTest()
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testUpdateNullabilityRowType(String formatType) {
        sql(
                "CREATE TABLE T "
                        + "( k INT, v ROW(f1 INT, f2 INT NOT NULL) NOT NULL, PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( 'bucket' = '1', 'file.format' = '"
                        + formatType
                        + "' )");
        sql("INSERT INTO T VALUES (1, ROW(10, 100)), (2, ROW(20, 200))");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, Row.of(10, 100)), Row.of(2, Row.of(20, 200)));

        sql("ALTER TABLE T MODIFY (v ROW(f1 INT, f2 INT) NOT NULL)"); // convert non nullable
        // field in row to
        // nullable
        sql("INSERT INTO T VALUES " + "(3, ROW(30, CAST(NULL AS INT)))");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, Row.of(10, 100)),
                        Row.of(2, Row.of(20, 200)),
                        Row.of(3, Row.of(30, null)));

        assertThatCode(() -> sql("ALTER TABLE T MODIFY (v ROW(f1 INT NOT NULL, f2 INT) NOT NULL)"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v.f1");

        sql("ALTER TABLE T MODIFY (v ROW(f1 INT, f2 INT))"); // convert entire row to nullable
        assertThatCode(() -> sql("ALTER TABLE T MODIFY (v ROW(f1 INT, f2 INT) NOT NULL)"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v");
    }

    @ParameterizedTest()
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testUpdateNullabilityArrayAndMapType(String formatType) {
        sql(
                "CREATE TABLE T "
                        + "( k INT, v1 ARRAY<ROW(f1 INT, f2 INT) NOT NULL>, v2 MAP<INT, ROW(f1 INT, f2 INT) NOT NULL>, PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( 'bucket' = '1', 'file.format' = '"
                        + formatType
                        + "' )");
        sql(
                "INSERT INTO T VALUES "
                        + "(1, ARRAY[ROW(10, 100), ROW(20, 200)], MAP[11, ROW(10, 100), 12, ROW(11, 110)]), "
                        + "(2, ARRAY[ROW(30, 300), ROW(40, 400)], MAP[21, ROW(20, 200), 22, ROW(21, 210)])");
        Map<Integer, Row> map1 = new HashMap<>();
        map1.put(11, Row.of(10, 100));
        map1.put(12, Row.of(11, 110));

        Map<Integer, Row> map2 = new HashMap<>();
        map2.put(21, Row.of(20, 200));
        map2.put(22, Row.of(21, 210));

        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, new Row[] {Row.of(10, 100), Row.of(20, 200)}, map1),
                        Row.of(2, new Row[] {Row.of(30, 300), Row.of(40, 400)}, map2));

        assertThatCode(
                        () ->
                                sql(
                                        "ALTER TABLE T MODIFY (v1 ARRAY<ROW(f1 INT, f2 INT) NOT NULL> NOT NULL)"))
                .hasRootCauseMessage(
                        "Cannot update column type from nullable to non nullable for v1. You can set table configuration option 'alter-column-null-to-not-null.disabled' = 'false' to allow converting null columns to not null");
        assertThatCode(
                        () ->
                                sql(
                                        "ALTER TABLE T MODIFY (v1 ARRAY<ROW(f1 INT NOT NULL, f2 INT) NOT NULL>)"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v1.element.f1");

        assertThatCode(
                        () ->
                                sql(
                                        "ALTER TABLE T MODIFY (v2 MAP<INT, ROW(f1 INT, f2 INT) NOT NULL> NOT NULL)"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v2");

        assertThatCode(
                        () ->
                                sql(
                                        "ALTER TABLE T MODIFY (v2 MAP<INT, ROW(f1 INT, f2 INT NOT NULL) NOT NULL>)"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v2.value.f2");
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testUpdateNullabilityByEnablingNullToNotNullOption(String formatType) {
        sql(
                "CREATE TABLE T "
                        + "( k INT, v INT, PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( 'bucket' = '1', 'file.format' = '"
                        + formatType
                        + "' )");

        sql("INSERT INTO T VALUES (1, 10), (2, 20)");
        assertThat(sql("SELECT * FROM T")).containsExactlyInAnyOrder(Row.of(1, 10), Row.of(2, 20));

        assertThatCode(() -> sql("ALTER TABLE T MODIFY v INT NOT NULL"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v");

        // enable null to not null option
        sql("ALTER TABLE T SET ('alter-column-null-to-not-null.disabled' = 'false')");
        sql("ALTER TABLE T MODIFY v INT NOT NULL");
        assertThat(sql("SELECT * FROM T")).containsExactlyInAnyOrder(Row.of(1, 10), Row.of(2, 20));
    }

    @Test
    public void testAlterColumnTypeWithNullabilityUpdate() {
        sql("CREATE TABLE T ( k INT, v INT, PRIMARY KEY(k) NOT ENFORCED )");

        sql("INSERT INTO T VALUES (1, 10), (2, 20)");
        assertThat(sql("SELECT * FROM T")).containsExactlyInAnyOrder(Row.of(1, 10), Row.of(2, 20));

        assertThatCode(() -> sql("ALTER TABLE T MODIFY v BIGINT NOT NULL"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v");

        // enable null to not null option
        sql("ALTER TABLE T SET ('alter-column-null-to-not-null.disabled' = 'false')");
        sql("ALTER TABLE T MODIFY v BIGINT NOT NULL");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 10L), Row.of(2, 20L));
    }

    @Test
    public void testAlterColumnTypeNestedArrayAndMap() {
        sql("CREATE TABLE T ( k INT, v ARRAY<ARRAY<ARRAY<INT>>>, PRIMARY KEY(k) NOT ENFORCED )");
        sql("INSERT INTO T VALUES (1, ARRAY[ARRAY[ARRAY[1, 2]]]), (2, ARRAY[ARRAY[ARRAY[3, 4]]])");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, new Integer[][][] {{{1, 2}}}),
                        Row.of(2, new Integer[][][] {{{3, 4}}}));
        sql("ALTER TABLE T MODIFY v ARRAY<ARRAY<ARRAY<BIGINT>>>");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, new Long[][][] {{{1L, 2L}}}),
                        Row.of(2, new Long[][][] {{{3L, 4L}}}));
        assertThatCode(() -> sql("ALTER TABLE T MODIFY v ARRAY<ARRAY<ARRAY<BIGINT NOT NULL>>>"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v.element.element.element");

        sql("DROP TABLE T");

        sql(
                "CREATE TABLE T ( k INT, v MAP<STRING, MAP<STRING, MAP<STRING, INT NOT NULL>>>, PRIMARY KEY(k) NOT ENFORCED )");
        Map<String, Map<String, Map<String, Integer>>> mp1 = new HashMap<>();
        Map<String, Map<String, Integer>> l1Mp1 = new HashMap<>();
        Map<String, Integer> l2Mp1 = new HashMap<>();
        l2Mp1.put("aaa", 1);
        l2Mp1.put("aab", 2);
        l1Mp1.put("aa", l2Mp1);
        mp1.put("a", l1Mp1);
        Map<String, Map<String, Map<String, Integer>>> mp2 = new HashMap<>();
        Map<String, Map<String, Integer>> l1Mp2 = new HashMap<>();
        Map<String, Integer> l2Mp2 = new HashMap<>();
        l2Mp2.put("bbb", 3);
        l2Mp2.put("bbc", 4);
        l1Mp2.put("bb", l2Mp2);
        mp2.put("b", l1Mp2);
        sql(
                "INSERT INTO T VALUES (1, MAP['a', MAP['aa', MAP['aaa', 1, 'aab', 2]]]), (2, MAP['b', MAP['bb', MAP['bbb', 3, 'bbc', 4]]])");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, mp1), Row.of(2, mp2));
        sql("ALTER TABLE T MODIFY v MAP<STRING, MAP<STRING, MAP<STRING, BIGINT>>>");
        Map<String, Map<String, Map<String, Long>>> mp3 = new HashMap<>();
        Map<String, Map<String, Long>> l1Mp3 = new HashMap<>();
        Map<String, Long> l2Mp3 = new HashMap<>();
        l2Mp3.put("aaa", 1L);
        l2Mp3.put("aab", 2L);
        l1Mp3.put("aa", l2Mp3);
        mp3.put("a", l1Mp3);
        Map<String, Map<String, Map<String, Long>>> mp4 = new HashMap<>();
        Map<String, Map<String, Long>> l1Mp4 = new HashMap<>();
        Map<String, Long> l2Mp4 = new HashMap<>();
        l2Mp4.put("bbb", 3L);
        l2Mp4.put("bbc", 4L);
        l1Mp4.put("bb", l2Mp4);
        mp4.put("b", l1Mp4);
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, mp3), Row.of(2, mp4));
        assertThatCode(
                        () ->
                                sql(
                                        "ALTER TABLE T MODIFY v MAP<STRING, MAP<STRING, MAP<STRING, BIGINT NOT NULL>>>"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for v.value.value.value");

        sql("DROP TABLE T");

        sql(
                "CREATE TABLE T ( k INT, a ARRAY<INT NOT NULL>, b MAP<STRING, INT NOT NULL>, PRIMARY KEY(k) NOT ENFORCED )");
        sql("INSERT INTO T VALUES (1, ARRAY[1, 2, 3, 4], MAP['a', 1, 'b', 2])");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                new Integer[] {1, 2, 3, 4},
                                new HashMap<String, Integer>() {
                                    {
                                        put("a", 1);
                                    }

                                    {
                                        put("b", 2);
                                    }
                                }));
        sql("ALTER TABLE T MODIFY a ARRAY<BIGINT>");
        sql("INSERT INTO T VALUES (2, ARRAY[5, 6, 7, CAST(NULL AS BIGINT)], MAP['c', 3, 'd', 4])");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                new Long[] {1L, 2L, 3L, 4L},
                                new HashMap<String, Integer>() {
                                    {
                                        put("a", 1);
                                    }

                                    {
                                        put("b", 2);
                                    }
                                }),
                        Row.of(
                                2,
                                new Long[] {5L, 6L, 7L, null},
                                new HashMap<String, Integer>() {
                                    {
                                        put("c", 3);
                                    }

                                    {
                                        put("d", 4);
                                    }
                                }));
        assertThatCode(() -> sql("ALTER TABLE T MODIFY a ARRAY<BIGINT NOT NULL>"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for a.element");
        sql("ALTER TABLE T MODIFY b MAP<STRING, BIGINT>");
        sql(
                "INSERT INTO T VALUES (2, ARRAY[5, 6, 7, CAST(NULL AS BIGINT)], MAP['c', 3, 'd', CAST(NULL AS BIGINT)])");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                new Long[] {1L, 2L, 3L, 4L},
                                new HashMap<String, Long>() {
                                    {
                                        put("a", 1L);
                                    }

                                    {
                                        put("b", 2L);
                                    }
                                }),
                        Row.of(
                                2,
                                new Long[] {5L, 6L, 7L, null},
                                new HashMap<String, Long>() {
                                    {
                                        put("c", 3L);
                                    }

                                    {
                                        put("d", null);
                                    }
                                }));
        assertThatCode(() -> sql("ALTER TABLE T MODIFY b MAP<STRING, BIGINT NOT NULL>"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for b.value");

        sql("DROP TABLE T");
        sql(
                "CREATE TABLE T ( k INT, a MAP<STRING, ARRAY<INT NOT NULL>>, PRIMARY KEY(k) NOT ENFORCED )");
        sql("INSERT INTO T VALUES (1, MAP['a', ARRAY[1, 2, 3]])");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                new HashMap<String, Integer[]>() {
                                    {
                                        put("a", new Integer[] {1, 2, 3});
                                    }
                                }));
        sql("ALTER TABLE T MODIFY a MAP<STRING, ARRAY<BIGINT>>");
        sql(
                "INSERT INTO T VALUES(1, MAP['a', ARRAY[1, 2, 3], 'b', ARRAY[2, 3, CAST(NULL AS BIGINT)]])");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(
                                1,
                                new HashMap<String, Long[]>() {
                                    {
                                        put("a", new Long[] {1L, 2L, 3L});
                                    }

                                    {
                                        put("b", new Long[] {2L, 3L, null});
                                    }
                                }));
        assertThatCode(() -> sql("ALTER TABLE T MODIFY a MAP<STRING, ARRAY<BIGINT NOT NULL>>"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for a.value.element");

        sql("DROP TABLE T");

        sql(
                "CREATE TABLE T ( k INT, a ROW(c1 DOUBLE, c2 ARRAY<BOOLEAN> NOT NULL) NOT NULL, PRIMARY KEY(k) NOT ENFORCED )");
        sql("INSERT INTO T VALUES (1, ROW(1.0, ARRAY[true, false]))");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, Row.of(1.0, new Boolean[] {true, false})));
        sql("ALTER TABLE T MODIFY a ROW(c1 DOUBLE, c2 ARRAY<BOOLEAN>) NOT NULL");
        sql("INSERT INTO T VALUES (2, ROW(2.0, CAST(NULL AS ARRAY<BOOLEAN>)))");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, Row.of(1.0, new Boolean[] {true, false})),
                        Row.of(2, Row.of(2.0, null)));
        assertThatCode(
                        () ->
                                sql(
                                        "ALTER TABLE T MODIFY a ROW(c1 DOUBLE, c2 ARRAY<BOOLEAN> NOT NULL) NOT NULL"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for a.c2");
        sql(
                "ALTER TABLE T MODIFY a ROW(c1 DOUBLE, c2 ARRAY<BOOLEAN>, c3 ARRAY<MAP<STRING, BOOLEAN NOT NULL>>) NOT NULL");
        sql(
                "ALTER TABLE T MODIFY a ROW(c1 DOUBLE, c2 ARRAY<BOOLEAN>, c3 ARRAY<MAP<STRING, BOOLEAN>>) NOT NULL");
        assertThatCode(
                        () ->
                                sql(
                                        "ALTER TABLE T MODIFY a ROW(c1 DOUBLE, c2 ARRAY<BOOLEAN>, c3 ARRAY<MAP<STRING, BOOLEAN NOT NULL>>) NOT NULL"))
                .hasStackTraceContaining(
                        "Cannot update column type from nullable to non nullable for a.c3.element.value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testDisableExplicitTypeCasting(String formatType) {
        sql(
                "CREATE TABLE T "
                        + "( k INT, v INT, PRIMARY KEY (k) NOT ENFORCED ) "
                        + "WITH ( 'bucket' = '1', 'file.format' = '"
                        + formatType
                        + "' )");
        sql("ALTER TABLE T SET ('disable-explicit-type-casting' = 'true')");
        sql("INSERT INTO T VALUES (1, 10), (2, 20)");
        assertThat(sql("SELECT * FROM T")).containsExactlyInAnyOrder(Row.of(1, 10), Row.of(2, 20));
        assertThatCode(() -> sql("ALTER TABLE T MODIFY v SMALLINT"))
                .hasStackTraceContaining(
                        "Column type v[INT] cannot be converted to SMALLINT without loosing information");
        sql("ALTER TABLE T MODIFY v BIGINT");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 10L), Row.of(2, 20L));
        assertThatCode(() -> sql("ALTER TABLE T MODIFY v INT"))
                .hasStackTraceContaining(
                        "Column type v[BIGINT] cannot be converted to INT without loosing information");
        // disable explicit type casting
        sql("ALTER TABLE T SET ('disable-explicit-type-casting' = 'false')");
        sql("ALTER TABLE T MODIFY v INT");
        assertThat(sql("SELECT * FROM T")).containsExactlyInAnyOrder(Row.of(1, 10), Row.of(2, 20));
    }
}
