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
import org.apache.atlas.repository.graphdb.AtlasVertex;

import java.util.Map;

public class AtlasAgeEdge extends AtlasAgeElement implements AtlasEdge<AtlasAgeVertex, AtlasAgeEdge> {
    private final AgeEdge edge;

    public AtlasAgeEdge(AtlasAgeGraph graph, AgeEdge edge) {
        super(graph);
        this.edge = edge;
    }

    @Override
    protected long getElementId() {
        return edge.getId();
    }

    @Override
    protected Map<String, Object> getElementProperties() {
        return edge.getProperties();
    }

    @Override
    protected boolean isVertex() {
        return false;
    }

    @Override
    protected boolean isElementRemoved() {
        return edge.isRemoved();
    }

    @Override
    public AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> getInVertex() {
        return graph.getVertex(String.valueOf(edge.getInVertexId()));
    }

    @Override
    public AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> getOutVertex() {
        return graph.getVertex(String.valueOf(edge.getOutVertexId()));
    }

    @Override
    public String getLabel() {
        return edge.getLabel();
    }

    @Override
    public AtlasAgeEdge getE() {
        return this;
    }

    public AgeEdge getWrappedEdge() {
        return edge;
    }

    @Override
    public String toString() {
        return "AtlasAgeEdge{id=" + edge.getId() + ", label='" + edge.getLabel() + "'}";
    }
}
