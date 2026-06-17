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
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.age.query.SolrToTsqueryParser;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AtlasAgeIndexQuery implements AtlasIndexQuery<AtlasAgeVertex, AtlasAgeEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAgeIndexQuery.class);

    private final AtlasAgeGraph graph;
    private final String indexName;
    private final String queryString;
    private final int offset;

    public AtlasAgeIndexQuery(AtlasAgeGraph graph, String indexName, String queryString, int offset) {
        this.graph = graph;
        this.indexName = indexName;
        this.queryString = queryString;
        this.offset = offset;
    }

    @Override
    public Iterator<Result<AtlasAgeVertex, AtlasAgeEdge>> vertices() {
        return vertices(0, Integer.MAX_VALUE, null, null);
    }

    @Override
    public Iterator<Result<AtlasAgeVertex, AtlasAgeEdge>> vertices(int offset, int limit) {
        return vertices(offset, limit, null, null);
    }

    @Override
    public Iterator<Result<AtlasAgeVertex, AtlasAgeEdge>> vertices(int offset, int limit, String sortBy, Order sortOrder) {
        try {
            List<Result<AtlasAgeVertex, AtlasAgeEdge>> results = new ArrayList<>();
            SolrToTsqueryParser.ParsedQuery parsed = SolrToTsqueryParser.parse(queryString);

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT vertex_id, properties, ");  // select props inline → no per-result query

            if (parsed.hasTsquery()) {
                sql.append("ts_rank_cd(search_text, to_tsquery('english', '")
                   .append(parsed.getTsquery()).append("')) AS score ");
            } else {
                sql.append("1.0 AS score ");
            }

            sql.append("FROM ag_catalog.atlas_fti_vertex WHERE 1=1");

            if (parsed.hasTsquery()) {
                sql.append(" AND search_text @@ to_tsquery('english', '")
                   .append(parsed.getTsquery()).append("')");
            }

            for (SolrToTsqueryParser.FieldFilter filter : parsed.getFieldFilters()) {
                sql.append(" AND properties->>'").append(filter.getField())
                   .append("' = '").append(filter.getValue().replace("'", "''")).append("'");
            }

            if (sortBy != null) {
                sql.append(" ORDER BY properties->>'").append(sortBy.replace("'", "''")).append("'");
                sql.append(sortOrder == Order.desc ? " DESC" : " ASC");
            } else if (parsed.hasTsquery()) {
                sql.append(" ORDER BY score DESC");
            }

            int effectiveOffset = this.offset + offset;
            if (effectiveOffset > 0) {
                sql.append(" OFFSET ").append(effectiveOffset);
            }
            if (limit > 0 && limit < Integer.MAX_VALUE) {
                sql.append(" LIMIT ").append(limit);
            }

            try (ResultSet rs = graph.getCypherExecutor().executeSql(sql.toString())) {
                while (rs.next()) {
                    long vertexId = rs.getLong(1);
                    String propsJson = rs.getString(2);
                    double score = rs.getDouble(3);

                    // Build the vertex inline from the row's properties (already selected above) —
                    // was N+1: a materializeVertex shadow query PER result (1000 hits = 1000 queries).
                    // Props must be present or Atlas's post-filter drops the row (__typeName/__state
                    // null -> empty search). [AGE-backend fork — inline materialization]
                    AgeVertex ageVertex = new AgeVertex(vertexId);
                    if (propsJson != null) {
                        ageVertex.setProperties(AgeCypherExecutor.parseAgtypeProperties(propsJson));
                    }
                    AtlasAgeVertex atlasVertex = new AtlasAgeVertex(graph, ageVertex);

                    results.add(new AgeIndexResult<>(atlasVertex, null, score));
                }
            }

            return results.iterator();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute index query", e);
        }
    }

    @Override
    public Long vertexTotals() {
        try {
            SolrToTsqueryParser.ParsedQuery parsed = SolrToTsqueryParser.parse(queryString);

            StringBuilder sql = new StringBuilder("SELECT count(*) FROM ag_catalog.atlas_fti_vertex WHERE 1=1");

            if (parsed.hasTsquery()) {
                sql.append(" AND search_text @@ to_tsquery('english', '")
                   .append(parsed.getTsquery()).append("')");
            }

            for (SolrToTsqueryParser.FieldFilter filter : parsed.getFieldFilters()) {
                sql.append(" AND properties->>'").append(filter.getField())
                   .append("' = '").append(filter.getValue().replace("'", "''")).append("'");
            }

            try (ResultSet rs = graph.getCypherExecutor().executeSql(sql.toString())) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }

            return 0L;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get vertex totals", e);
        }
    }

    @Override
    public Iterator<Result<AtlasAgeVertex, AtlasAgeEdge>> edges() {
        return edges(0, Integer.MAX_VALUE, null, null);
    }

    @Override
    public Iterator<Result<AtlasAgeVertex, AtlasAgeEdge>> edges(int offset, int limit) {
        return edges(offset, limit, null, null);
    }

    @Override
    public Iterator<Result<AtlasAgeVertex, AtlasAgeEdge>> edges(int offset, int limit, String sortBy, Order sortOrder) {
        try {
            List<Result<AtlasAgeVertex, AtlasAgeEdge>> results = new ArrayList<>();
            SolrToTsqueryParser.ParsedQuery parsed = SolrToTsqueryParser.parse(queryString);

            StringBuilder sql = new StringBuilder("SELECT edge_id, 1.0 AS score FROM ag_catalog.atlas_fti_edge WHERE 1=1");

            for (SolrToTsqueryParser.FieldFilter filter : parsed.getFieldFilters()) {
                sql.append(" AND properties->>'").append(filter.getField())
                   .append("' = '").append(filter.getValue().replace("'", "''")).append("'");
            }

            int effectiveOffset = this.offset + offset;
            if (effectiveOffset > 0) sql.append(" OFFSET ").append(effectiveOffset);
            if (limit > 0 && limit < Integer.MAX_VALUE) sql.append(" LIMIT ").append(limit);

            try (ResultSet rs = graph.getCypherExecutor().executeSql(sql.toString())) {
                while (rs.next()) {
                    long edgeId = rs.getLong(1);
                    double score = rs.getDouble(2);

                    AtlasEdge<AtlasAgeVertex, AtlasAgeEdge> edge = graph.getEdge(String.valueOf(edgeId));
                    if (edge != null) {
                        results.add(new AgeIndexResult<>(null, (AtlasAgeEdge) edge, score));
                    }
                }
            }

            return results.iterator();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute edge index query", e);
        }
    }

    @Override
    public Long edgeTotals() {
        try {
            String sql = "SELECT count(*) FROM ag_catalog.atlas_fti_edge";
            try (ResultSet rs = graph.getCypherExecutor().executeSql(sql)) {
                if (rs.next()) return rs.getLong(1);
            }
            return 0L;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get edge totals", e);
        }
    }

    private static class AgeIndexResult<V, E> implements Result<V, E> {
        private final AtlasVertex<V, E> vertex;
        private final AtlasEdge<V, E> edge;
        private final double score;

        @SuppressWarnings("unchecked")
        AgeIndexResult(Object vertex, Object edge, double score) {
            this.vertex = (AtlasVertex<V, E>) vertex;
            this.edge = (AtlasEdge<V, E>) edge;
            this.score = score;
        }

        @Override public AtlasVertex<V, E> getVertex() { return vertex; }
        @Override public AtlasEdge<V, E> getEdge() { return edge; }
        @Override public double getScore() { return score; }
    }
}
