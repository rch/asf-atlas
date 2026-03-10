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

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AtlasAgeVertex extends AtlasAgeElement implements AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> {
    private final AgeVertex vertex;

    public AtlasAgeVertex(AtlasAgeGraph graph, AgeVertex vertex) {
        super(graph);
        this.vertex = vertex;
    }

    @Override
    protected long getElementId() {
        return vertex.getId();
    }

    @Override
    protected Map<String, Object> getElementProperties() {
        return vertex.getProperties();
    }

    @Override
    protected boolean isVertex() {
        return true;
    }

    @Override
    protected boolean isElementRemoved() {
        return vertex.isRemoved();
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> getEdges(AtlasEdgeDirection direction, String edgeLabel) {
        try {
            return graph.getEdgesForVertex(vertex.getId(), direction, edgeLabel);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get edges for vertex " + vertex.getId(), e);
        }
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> getEdges(AtlasEdgeDirection direction, String[] edgeLabels) {
        List<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();

        for (String label : edgeLabels) {
            for (AtlasEdge<AtlasAgeVertex, AtlasAgeEdge> edge : getEdges(direction, label)) {
                result.add(edge);
            }
        }

        return result;
    }

    @Override
    public long getEdgesCount(AtlasEdgeDirection direction, String edgeLabel) {
        try {
            return graph.getEdgesCountForVertex(vertex.getId(), direction, edgeLabel);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count edges for vertex " + vertex.getId(), e);
        }
    }

    @Override
    public boolean hasEdges(AtlasEdgeDirection direction, String edgeLabel) {
        return getEdgesCount(direction, edgeLabel) > 0;
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> getEdges(AtlasEdgeDirection direction) {
        return getEdges(direction, (String) null);
    }

    @Override
    public <T> void addProperty(String propertyName, T value) {
        try {
            Object existing = getElementProperties().get(propertyName);
            if (existing instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) existing;
                if (!list.contains(value)) {
                    list.add(value);
                    setProperty(propertyName, list);
                }
            } else if (existing == null) {
                List<Object> list = new ArrayList<>();
                list.add(value);
                setProperty(propertyName, list);
            }
            // If existing is not a list and not null, this is SET semantics — no duplicate added
        } catch (Exception e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public <T> void addListProperty(String propertyName, T value) {
        try {
            Object existing = getElementProperties().get(propertyName);
            if (existing instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> list = new ArrayList<>((List<Object>) existing);
                list.add(value);
                setProperty(propertyName, list);
            } else {
                List<Object> list = new ArrayList<>();
                list.add(value);
                setProperty(propertyName, list);
            }
        } catch (Exception e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public AtlasVertexQuery<AtlasAgeVertex, AtlasAgeEdge> query() {
        return new AtlasAgeVertexQuery(graph, this);
    }

    @Override
    public AtlasAgeVertex getV() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Collection<T> getPropertyValues(String propertyName, Class<T> type) {
        Object value = getElementProperties().get(propertyName);

        if (value == null) {
            return new ArrayList<>();
        }

        if (value instanceof List) {
            List<T> result = new ArrayList<>();
            for (Object item : (List<?>) value) {
                result.add((T) item);
            }
            return result;
        }

        List<T> result = new ArrayList<>();
        result.add((T) value);
        return result;
    }

    public AgeVertex getWrappedVertex() {
        return vertex;
    }

    @Override
    public String toString() {
        return "AtlasAgeVertex{id=" + vertex.getId() + "}";
    }
}
