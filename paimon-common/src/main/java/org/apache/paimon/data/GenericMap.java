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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link MapType} or {@link MultisetType}.
 *
 * <p>{@link GenericMap} is a generic implementation of {@link InternalMap} which wraps regular Java
 * maps.
 *
 * <p>Note: All keys and values of this data structure must be internal data structures. All keys
 * must be of the same type; same for values. See {@link InternalRow} for more information about
 * internal data structures.
 *
 * <p>Both keys and values can contain null for representing nullability.
 *
 * @since 0.4.0
 */
@Public
public final class GenericMap implements InternalMap, Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<?, ?> map;

    /**
     * Creates an instance of {@link GenericMap} using the given Java map.
     *
     * <p>Note: All keys and values of the map must be internal data structures.
     */
    public GenericMap(Map<?, ?> map) {
        this.map = map;
    }

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this map contains
     * no mapping for the key. The returned value is in internal data structure.
     */
    public Object get(Object key) {
        return map.get(key);
    }

    public boolean contains(Object key) {
        return map.containsKey(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public InternalArray keyArray() {
        Object[] keys = map.keySet().toArray();
        return new GenericArray(keys);
    }

    @Override
    public InternalArray valueArray() {
        Object[] values = map.values().toArray();
        return new GenericArray(values);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof GenericMap)) {
            return false;
        }
        // deepEquals for values of byte[]
        return deepEquals(map, ((GenericMap) o).map);
    }

    private static <K, V> boolean deepEquals(Map<K, V> m1, Map<?, ?> m2) {
        // copied from HashMap.equals but with deepEquals comparison
        if (m1.size() != m2.size()) {
            return false;
        }
        try {
            for (Map.Entry<K, V> e : m1.entrySet()) {
                K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(m2.get(key) == null && m2.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!Objects.deepEquals(value, m2.get(key))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Object key : map.keySet()) {
            // only include key because values can contain byte[]
            result += 31 * Objects.hashCode(key);
        }
        return result;
    }
}
