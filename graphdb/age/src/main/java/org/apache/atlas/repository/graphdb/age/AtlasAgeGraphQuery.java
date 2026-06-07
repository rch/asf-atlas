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
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.age.query.AgeQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AtlasAgeGraphQuery implements AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAgeGraphQuery.class);

    private final AtlasAgeGraph graph;
    private final AgeQueryBuilder queryBuilder;
    private final boolean isChildQuery;

    public AtlasAgeGraphQuery(AtlasAgeGraph graph) {
        this(graph, false);
    }

    private AtlasAgeGraphQuery(AtlasAgeGraph graph, boolean isChildQuery) {
        this.graph = graph;
        this.queryBuilder = new AgeQueryBuilder();
        this.isChildQuery = isChildQuery;
    }

    @Override
    public AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> has(String propertyKey, Object value) {
        queryBuilder.has(propertyKey, value);
        return this;
    }

    @Override
    public AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> in(String propertyKey, Collection<?> values) {
        queryBuilder.in(propertyKey, values);
        return this;
    }

    @Override
    public AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> has(String propertyKey, QueryOperator op, Object values) {
        queryBuilder.has(propertyKey, op, values);
        return this;
    }

    @Override
    public AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> orderBy(String propertyKey, SortOrder order) {
        queryBuilder.orderBy(propertyKey, order);
        return this;
    }

    @Override
    public AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> or(List<AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge>> childQueries) {
        List<AgeQueryBuilder> childBuilders = new ArrayList<>();
        for (AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> q : childQueries) {
            childBuilders.add(((AtlasAgeGraphQuery) q).queryBuilder);
        }
        queryBuilder.or(childBuilders);
        return this;
    }

    @Override
    public AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> createChildQuery() {
        return new AtlasAgeGraphQuery(graph, true);
    }

    @Override
    public AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> addConditionsFrom(AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> otherQuery) {
        queryBuilder.addConditionsFrom(((AtlasAgeGraphQuery) otherQuery).queryBuilder);
        return this;
    }

    @Override
    public boolean isChildQuery() {
        return isChildQuery;
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> edges() {
        return edges(0, -1);
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> edges(int limit) {
        return edges(0, limit);
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> edges(int offset, int limit) {
        try {
            List<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();
            String whereClause = queryBuilder.buildWhereClause("e");
            String cypher = "MATCH (a:vertex)-[e]->(b:vertex)" +
                    (whereClause.isEmpty() ? "" : " WHERE " + whereClause) +
                    " RETURN id(e), id(a), id(b), label(e)" +
                    queryBuilder.buildOrderBy("e") +
                    buildPagination(offset, limit);

            try (ResultSet rs = graph.getCypherExecutor().executeCypherWithTypes(cypher,
                    "eid agtype, aid agtype, bid agtype, lbl agtype")) {
                while (rs.next()) {
                    long eid = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                    long aid = AgeCypherExecutor.extractAgtypeId(rs.getString(2));
                    long bid = AgeCypherExecutor.extractAgtypeId(rs.getString(3));
                    String label = rs.getString(4);
                    if (label != null) label = label.replace("\"", "").trim();

                    result.add(new AtlasAgeEdge(graph, new AgeEdge(eid, aid, bid, label)));
                }
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute edge query", e);
        }
    }

    @Override
    public Iterable<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> vertices() {
        return vertices(0, -1);
    }

    @Override
    public Iterable<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> vertices(int limit) {
        return vertices(0, limit);
    }

    @Override
    public Iterable<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> vertices(int offset, int limit) {
        try {
            List<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();
            String whereClause = queryBuilder.buildWhereClause("n");
            String cypher = "MATCH (n:vertex)" +
                    (whereClause.isEmpty() ? "" : " WHERE " + whereClause) +
                    " RETURN id(n)" +
                    queryBuilder.buildOrderBy("n") +
                    buildPagination(offset, limit);

            try (ResultSet rs = graph.getCypherExecutor().executeCypher(cypher)) {
                while (rs.next()) {
                    long vid = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                    result.add(graph.materializeVertex(vid));  // load shadow props (else __typeName null)
                }
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute vertex query", e);
        }
    }

    @Override
    public Iterable<Object> vertexIds() {
        return vertexIds(0, -1);
    }

    @Override
    public Iterable<Object> vertexIds(int limit) {
        return vertexIds(0, limit);
    }

    @Override
    public Iterable<Object> vertexIds(int offset, int limit) {
        try {
            List<Object> result = new ArrayList<>();
            String whereClause = queryBuilder.buildWhereClause("n");
            String cypher = "MATCH (n:vertex)" +
                    (whereClause.isEmpty() ? "" : " WHERE " + whereClause) +
                    " RETURN id(n)" +
                    queryBuilder.buildOrderBy("n") +
                    buildPagination(offset, limit);

            try (ResultSet rs = graph.getCypherExecutor().executeCypher(cypher)) {
                while (rs.next()) {
                    result.add(AgeCypherExecutor.extractAgtypeId(rs.getString(1)));
                }
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute vertex ID query", e);
        }
    }

    private String buildPagination(int offset, int limit) {
        StringBuilder sb = new StringBuilder();
        if (offset > 0) sb.append(" SKIP ").append(offset);
        if (limit > 0) sb.append(" LIMIT ").append(limit);
        return sb.toString();
    }

    AgeQueryBuilder getQueryBuilder() {
        return queryBuilder;
    }
}
