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
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AtlasAgeVertexQuery implements AtlasVertexQuery<AtlasAgeVertex, AtlasAgeEdge> {
    private final AtlasAgeGraph graph;
    private final AtlasAgeVertex vertex;
    private AtlasEdgeDirection direction = AtlasEdgeDirection.BOTH;
    private String edgeLabel;
    private final Map<String, Object> hasConditions = new HashMap<>();

    public AtlasAgeVertexQuery(AtlasAgeGraph graph, AtlasAgeVertex vertex) {
        this.graph = graph;
        this.vertex = vertex;
    }

    @Override
    public AtlasVertexQuery<AtlasAgeVertex, AtlasAgeEdge> direction(AtlasEdgeDirection queryDirection) {
        this.direction = queryDirection;
        return this;
    }

    @Override
    public AtlasVertexQuery<AtlasAgeVertex, AtlasAgeEdge> label(String label) {
        this.edgeLabel = label;
        return this;
    }

    @Override
    public AtlasVertexQuery<AtlasAgeVertex, AtlasAgeEdge> has(String key, Object value) {
        hasConditions.put(key, value);
        return this;
    }

    @Override
    public Iterable<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> vertices() {
        return vertices(-1);
    }

    @Override
    public Iterable<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> vertices(int limit) {
        try {
            List<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();
            String cypher = buildMatchCypher("m") +
                    (limit > 0 ? " LIMIT " + limit : "") +
                    " RETURN DISTINCT id(m)";

            try (ResultSet rs = graph.getCypherExecutor().executeCypher(cypher)) {
                while (rs.next()) {
                    long mid = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                    result.add(new AtlasAgeVertex(graph, new AgeVertex(mid)));
                }
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute vertex query", e);
        }
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> edges() {
        return edges(-1);
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> edges(int limit) {
        try {
            List<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();
            String cypher = buildMatchCypher("e") +
                    (limit > 0 ? " LIMIT " + limit : "") +
                    " RETURN id(e), id(startNode(e)), id(endNode(e)), label(e)";

            try (ResultSet rs = graph.getCypherExecutor().executeCypherWithTypes(cypher,
                    "eid agtype, sid agtype, tid agtype, lbl agtype")) {
                while (rs.next()) {
                    long eid = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                    long sid = AgeCypherExecutor.extractAgtypeId(rs.getString(2));
                    long tid = AgeCypherExecutor.extractAgtypeId(rs.getString(3));
                    String label = rs.getString(4);
                    if (label != null) label = label.replace("\"", "").trim();

                    result.add(new AtlasAgeEdge(graph, new AgeEdge(eid, sid, tid, label)));
                }
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute edge query", e);
        }
    }

    @Override
    public long count() {
        try {
            String cypher = buildMatchCypher("e") + " RETURN count(e)";

            try (ResultSet rs = graph.getCypherExecutor().executeCypher(cypher)) {
                if (rs.next()) {
                    return AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                }
            }

            return 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count edges", e);
        }
    }

    private String buildMatchCypher(String returnAlias) {
        long vertexId = vertex.getWrappedVertex().getId();
        String labelPart = edgeLabel != null ? ":" + AgeCypherExecutor.escapeCypherLabel(edgeLabel) : "";

        String matchPattern;
        switch (direction) {
            case OUT:
                matchPattern = "(a:vertex)-[e" + labelPart + "]->(m:vertex)";
                break;
            case IN:
                matchPattern = "(m:vertex)-[e" + labelPart + "]->(a:vertex)";
                break;
            default:
                matchPattern = "(a:vertex)-[e" + labelPart + "]-(m:vertex)";
                break;
        }

        StringBuilder sb = new StringBuilder("MATCH " + matchPattern + " WHERE id(a) = " + vertexId);

        for (Map.Entry<String, Object> cond : hasConditions.entrySet()) {
            sb.append(" AND e.").append(AgeCypherExecutor.escapeCypherKey(cond.getKey()))
              .append(" = ").append(AgeCypherExecutor.toCypherValue(cond.getValue()));
        }

        return sb.toString();
    }
}
