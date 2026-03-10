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
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

@SuppressWarnings({"unchecked", "rawtypes"})
public class AtlasAgeGraphTraversal extends AtlasGraphTraversal<AtlasVertex<?, ?>, AtlasEdge<?, ?>> {
    private static final TinkerGraph DUMMY_GRAPH = TinkerGraph.open();

    private final AtlasAgeGraph ageGraph;
    private final Object[] seedIds;
    private final boolean isEdgeTraversal;

    public AtlasAgeGraphTraversal(AtlasAgeGraph ageGraph, Object... vertexIds) {
        this(ageGraph, false, vertexIds);
    }

    public AtlasAgeGraphTraversal(AtlasAgeGraph ageGraph, boolean isEdge, Object... ids) {
        super((AtlasGraph) ageGraph, DUMMY_GRAPH);
        this.ageGraph = ageGraph;
        this.seedIds = ids;
        this.isEdgeTraversal = isEdge;
    }

    // Anonymous traversal constructor
    private AtlasAgeGraphTraversal() {
        super();
        this.ageGraph = null;
        this.seedIds = null;
        this.isEdgeTraversal = false;
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex<?, ?>, AtlasEdge<?, ?>> startAnonymousTraversal() {
        return new AtlasAgeGraphTraversal();
    }

    @Override
    public List<AtlasVertex<?, ?>> getAtlasVertexList() {
        List<AtlasVertex<?, ?>> result = new ArrayList<>();

        if (seedIds != null && seedIds.length > 0) {
            for (Object id : seedIds) {
                AtlasVertex vertex = ageGraph.getVertex(id.toString());
                if (vertex != null) {
                    result.add(vertex);
                }
            }
        }

        return result;
    }

    @Override
    public Set<AtlasVertex<?, ?>> getAtlasVertexSet() {
        return new HashSet<>(getAtlasVertexList());
    }

    @Override
    public Map<String, Collection<AtlasVertex<?, ?>>> getAtlasVertexMap() {
        Map<String, Collection<AtlasVertex<?, ?>>> result = new HashMap<>();

        for (AtlasVertex<?, ?> vertex : getAtlasVertexList()) {
            String typeName = vertex.getProperty("__typeName", String.class);
            if (typeName != null) {
                result.computeIfAbsent(typeName, k -> new ArrayList<>()).add(vertex);
            }
        }

        return result;
    }

    @Override
    public Set<AtlasEdge<?, ?>> getAtlasEdgeSet() {
        Set<AtlasEdge<?, ?>> result = new HashSet<>();

        if (isEdgeTraversal && seedIds != null) {
            for (Object id : seedIds) {
                AtlasEdge edge = ageGraph.getEdge(id.toString());
                if (edge != null) {
                    result.add(edge);
                }
            }
        }

        return result;
    }

    @Override
    public Map<String, AtlasEdge<?, ?>> getAtlasEdgeMap() {
        Map<String, AtlasEdge<?, ?>> result = new HashMap<>();

        for (AtlasEdge<?, ?> edge : getAtlasEdgeSet()) {
            result.put(edge.getLabel(), edge);
        }

        return result;
    }

    @Override
    public TextPredicate textPredicate() {
        return new AgeTextPredicate();
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex<?, ?>, AtlasEdge<?, ?>> textRegEx(String key, String value) {
        // Filtering via Cypher regex would happen here in a full implementation
        return this;
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex<?, ?>, AtlasEdge<?, ?>> textContainsRegEx(String value, String removeRedundantQuotes) {
        return this;
    }

    private static class AgeTextPredicate implements TextPredicate {
        @Override
        public BiPredicate<?, ?> contains() {
            return (BiPredicate<String, String>) (text, term) ->
                    text != null && text.toLowerCase().contains(term.toLowerCase());
        }

        @Override
        public BiPredicate<?, ?> containsPrefix() {
            return (BiPredicate<String, String>) (text, prefix) ->
                    text != null && text.toLowerCase().startsWith(prefix.toLowerCase());
        }

        @Override
        public BiPredicate<?, ?> containsRegex() {
            return (BiPredicate<String, String>) (text, regex) ->
                    text != null && text.matches(regex);
        }

        @Override
        public BiPredicate<?, ?> prefix() {
            return (BiPredicate<String, String>) (text, prefix) ->
                    text != null && text.startsWith(prefix);
        }

        @Override
        public BiPredicate<?, ?> regex() {
            return (BiPredicate<String, String>) (text, regex) ->
                    text != null && text.matches(regex);
        }
    }
}
