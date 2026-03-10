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

import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

public class AtlasAgePropertyKey implements AtlasPropertyKey {
    private final String name;
    private final AtlasCardinality cardinality;

    public AtlasAgePropertyKey(String name, AtlasCardinality cardinality) {
        this.name = name;
        this.cardinality = cardinality;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public AtlasCardinality getCardinality() {
        return cardinality;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasAgePropertyKey that = (AtlasAgePropertyKey) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
