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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Data type of a sequence of fields. A field consists of a field name, field type, and an optional
 * description. The most specific type of a row of a table is a row type. In this case, each column
 * of the row corresponds to the field of the row type that has the same ordinal position as the
 * column. Compared to the SQL standard, an optional field description simplifies the handling with
 * complex structures.
 *
 * @since 0.4.0
 */
@Public
public final class RowType extends DataType {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_FIELDS = "fields";

    public static final String FORMAT = "ROW<%s>";

    private final List<DataField> fields;

    public RowType(boolean isNullable, List<DataField> fields) {
        super(isNullable, DataTypeRoot.ROW);
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(fields, "Fields must not be null.")));

        validateFields(fields);
    }

    @JsonCreator
    public RowType(@JsonProperty(FIELD_FIELDS) List<DataField> fields) {
        this(true, fields);
    }

    public RowType copy(List<DataField> newFields) {
        return new RowType(isNullable(), newFields);
    }

    public List<DataField> getFields() {
        return fields;
    }

    public List<String> getFieldNames() {
        return fields.stream().map(DataField::name).collect(Collectors.toList());
    }

    public List<DataType> getFieldTypes() {
        return fields.stream().map(DataField::type).collect(Collectors.toList());
    }

    public DataType getTypeAt(int i) {
        return fields.get(i).type();
    }

    public int getFieldCount() {
        return fields.size();
    }

    public int getFieldIndex(String fieldName) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    public int[] getFieldIndices(List<String> projectFields) {
        List<String> fieldNames = getFieldNames();
        int[] projection = new int[projectFields.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = fieldNames.indexOf(projectFields.get(i));
        }
        return projection;
    }

    public boolean containsField(String fieldName) {
        for (DataField field : fields) {
            if (field.name().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsField(int fieldId) {
        for (DataField field : fields) {
            if (field.id() == fieldId) {
                return true;
            }
        }
        return false;
    }

    public boolean notContainsField(String fieldName) {
        return !containsField(fieldName);
    }

    public DataField getField(String fieldName) {
        for (DataField field : fields) {
            if (field.name().equals(fieldName)) {
                return field;
            }
        }

        throw new RuntimeException("Cannot find field: " + fieldName);
    }

    public DataField getField(int fieldId) {
        for (DataField field : fields) {
            if (field.id() == fieldId) {
                return field;
            }
        }
        throw new RuntimeException("Cannot find field by field id: " + fieldId);
    }

    public int getFieldIndexByFieldId(int fieldId) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).id() == fieldId) {
                return i;
            }
        }
        throw new RuntimeException("Cannot find field index by FieldId " + fieldId);
    }

    @Override
    public int defaultSize() {
        return fields.stream().mapToInt(f -> f.type().defaultSize()).sum();
    }

    @Override
    public RowType copy(boolean isNullable) {
        return new RowType(
                isNullable, fields.stream().map(DataField::copy).collect(Collectors.toList()));
    }

    @Override
    public RowType notNull() {
        return copy(false);
    }

    @Override
    public String asSQLString() {
        return withNullability(
                FORMAT,
                fields.stream().map(DataField::asSQLString).collect(Collectors.joining(", ")));
    }

    @Override
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", isNullable() ? "ROW" : "ROW NOT NULL");
        generator.writeArrayFieldStart("fields");
        for (DataField field : getFields()) {
            field.serializeJson(generator);
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType rowType = (RowType) o;
        return fields.equals(rowType.fields);
    }

    @Override
    public boolean equalsIgnoreFieldId(DataType o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType other = (RowType) o;
        if (fields.size() != other.fields.size()) {
            return false;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!fields.get(i).equalsIgnoreFieldId(other.fields.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isPrunedFrom(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType rowType = (RowType) o;
        for (DataField field : fields) {
            if (!field.isPrunedFrom(rowType.getField(field.id()))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    private static void validateFields(List<DataField> fields) {
        final List<String> fieldNames =
                fields.stream().map(DataField::name).collect(Collectors.toList());
        if (fieldNames.stream().anyMatch(StringUtils::isNullOrWhitespaceOnly)) {
            throw new IllegalArgumentException(
                    "Field names must contain at least one non-whitespace character.");
        }
        final Set<String> duplicates =
                fieldNames.stream()
                        .filter(n -> Collections.frequency(fieldNames, n) > 1)
                        .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Field names must be unique. Found duplicates: %s", duplicates));
        }
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void collectFieldIds(Set<Integer> fieldIds) {
        for (DataField field : fields) {
            if (fieldIds.contains(field.id())) {
                throw new RuntimeException(
                        String.format("Broken schema, field id %s is duplicated.", field.id()));
            }
            fieldIds.add(field.id());
            field.type().collectFieldIds(fieldIds);
        }
    }

    public RowType appendDataField(String name, DataType type) {
        List<DataField> newFields = new ArrayList<>(fields);
        int newId = currentHighestFieldId(fields) + 1;
        newFields.add(new DataField(newId, name, type));
        return new RowType(newFields);
    }

    public RowType project(int[] mapping) {
        List<DataField> fields = getFields();
        return new RowType(
                        Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()))
                .copy(isNullable());
    }

    public RowType project(List<String> names) {
        List<DataField> fields = getFields();
        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());
        return new RowType(
                        names.stream()
                                .map(k -> fields.get(fieldNames.indexOf(k)))
                                .collect(Collectors.toList()))
                .copy(isNullable());
    }

    public RowType project(String... names) {
        return project(Arrays.asList(names));
    }

    public static RowType of() {
        return new RowType(true, Collections.emptyList());
    }

    public static RowType of(DataField... fields) {
        final List<DataField> fs = new ArrayList<>(Arrays.asList(fields));
        return new RowType(true, fs);
    }

    public static RowType of(DataType... types) {
        final List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new DataField(i, "f" + i, types[i]));
        }
        return new RowType(true, fields);
    }

    public static RowType of(DataType[] types, String[] names) {
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new DataField(i, names[i], types[i]));
        }
        return new RowType(true, fields);
    }

    public static int currentHighestFieldId(List<DataField> fields) {
        Set<Integer> fieldIds = new HashSet<>();
        new RowType(fields).collectFieldIds(fieldIds);
        return fieldIds.stream()
                .filter(i -> !SpecialFields.isSystemField(i))
                .max(Integer::compareTo)
                .orElse(-1);
    }

    public static Builder builder() {
        return builder(new AtomicInteger(-1));
    }

    public static Builder builder(AtomicInteger fieldId) {
        return builder(true, fieldId);
    }

    public static Builder builder(boolean isNullable, AtomicInteger fieldId) {
        return new Builder(isNullable, fieldId);
    }

    /** Builder of {@link RowType}. */
    public static class Builder {

        private final List<DataField> fields = new ArrayList<>();

        private final boolean isNullable;
        private final AtomicInteger fieldId;

        private Builder(boolean isNullable, AtomicInteger fieldId) {
            this.isNullable = isNullable;
            this.fieldId = fieldId;
        }

        public Builder field(String name, DataType type) {
            fields.add(new DataField(fieldId.incrementAndGet(), name, type));
            return this;
        }

        public Builder field(String name, DataType type, @Nullable String description) {
            fields.add(new DataField(fieldId.incrementAndGet(), name, type, description));
            return this;
        }

        public Builder field(
                String name,
                DataType type,
                @Nullable String description,
                @Nullable String defaultValue) {
            fields.add(
                    new DataField(
                            fieldId.incrementAndGet(), name, type, description, defaultValue));
            return this;
        }

        public Builder fields(List<DataType> types) {
            for (int i = 0; i < types.size(); i++) {
                field("f" + i, types.get(i));
            }
            return this;
        }

        public Builder fields(DataType... types) {
            for (int i = 0; i < types.length; i++) {
                field("f" + i, types[i]);
            }
            return this;
        }

        public Builder fields(DataType[] types, String[] names) {
            for (int i = 0; i < types.length; i++) {
                field(names[i], types[i]);
            }
            return this;
        }

        public RowType build() {
            return new RowType(isNullable, fields);
        }
    }
}
