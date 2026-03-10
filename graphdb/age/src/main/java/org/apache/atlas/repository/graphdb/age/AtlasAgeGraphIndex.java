/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.age;

import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

import java.util.Set;

public class AtlasAgeGraphIndex implements AtlasGraphIndex {
    private final String name;
    private final boolean compositeIndex;
    private final boolean vertexIndex;
    private final boolean unique;
    private final Set<AtlasPropertyKey> fieldKeys;

    public AtlasAgeGraphIndex(String name, boolean compositeIndex, boolean vertexIndex,
                              boolean unique, Set<AtlasPropertyKey> fieldKeys) {
        this.name = name;
        this.compositeIndex = compositeIndex;
        this.vertexIndex = vertexIndex;
        this.unique = unique;
        this.fieldKeys = fieldKeys;
    }

    @Override
    public boolean isMixedIndex() {
        return !compositeIndex;
    }

    @Override
    public boolean isCompositeIndex() {
        return compositeIndex;
    }

    @Override
    public boolean isEdgeIndex() {
        return !vertexIndex;
    }

    @Override
    public boolean isVertexIndex() {
        return vertexIndex;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public Set<AtlasPropertyKey> getFieldKeys() {
        return fieldKeys;
    }

    public String getName() {
        return name;
    }

    void addFieldKey(AtlasPropertyKey key) {
        fieldKeys.add(key);
    }
}
