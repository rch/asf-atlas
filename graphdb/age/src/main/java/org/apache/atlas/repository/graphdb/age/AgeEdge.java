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

import java.util.HashMap;
import java.util.Map;

public class AgeEdge {
    private final long id;
    private final long outVertexId;
    private final long inVertexId;
    private final String label;
    private Map<String, Object> properties;
    private boolean removed;

    public AgeEdge(long id, long outVertexId, long inVertexId, String label) {
        this(id, outVertexId, inVertexId, label, new HashMap<>());
    }

    public AgeEdge(long id, long outVertexId, long inVertexId, String label, Map<String, Object> properties) {
        this.id = id;
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
        this.properties = properties != null ? properties : new HashMap<>();
        this.removed = false;
    }

    public long getId() {
        return id;
    }

    public long getOutVertexId() {
        return outVertexId;
    }

    public long getInVertexId() {
        return inVertexId;
    }

    public String getLabel() {
        return label;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    public boolean isRemoved() {
        return removed;
    }

    public void setRemoved(boolean removed) {
        this.removed = removed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AgeEdge that = (AgeEdge) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public String toString() {
        return "AgeEdge{id=" + id + ", label='" + label + "'}";
    }
}
