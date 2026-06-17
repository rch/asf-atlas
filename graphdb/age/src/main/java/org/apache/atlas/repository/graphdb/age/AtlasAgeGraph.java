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

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQueryParameter;
import org.apache.atlas.repository.graphdb.AtlasUniqueKeyHandler;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GraphIndexQueryParameters;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AtlasAgeGraph implements AtlasGraph<AtlasAgeVertex, AtlasAgeEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAgeGraph.class);

    private final AgeConnectionPool    connectionPool;
    private final AgeTransactionManager txManager;
    private final AgeCypherExecutor     cypherExecutor;
    private final AgeSchemaManager      schemaManager;
    private final Set<String>           multiProperties = new HashSet<>();
    private final AtlasAgeUniqueKeyHandler uniqueKeyHandler;

    public AtlasAgeGraph(AgeConnectionPool connectionPool, String graphName) {
        this.connectionPool = connectionPool;
        this.txManager      = new AgeTransactionManager(connectionPool);
        this.cypherExecutor = new AgeCypherExecutor(txManager, graphName);
        this.schemaManager  = new AgeSchemaManager(connectionPool, graphName);
        this.uniqueKeyHandler = new AtlasAgeUniqueKeyHandler(cypherExecutor);

        schemaManager.initializeSchema();
    }

    @Override
    public AtlasEdge<AtlasAgeVertex, AtlasAgeEdge> addEdge(AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> outVertex,
                                                             AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> inVertex,
                                                             String label) {
        try {
            long outId = ((AtlasAgeVertex) outVertex).getWrappedVertex().getId();
            long inId  = ((AtlasAgeVertex) inVertex).getWrappedVertex().getId();
            long edgeId = cypherExecutor.createEdge(outId, inId, label);

            AgeEdge ageEdge = new AgeEdge(edgeId, outId, inId, label);
            AtlasAgeEdge atlasEdge = new AtlasAgeEdge(this, ageEdge);

            cypherExecutor.syncEdgeToShadow(edgeId, Collections.singletonMap("__label", label));

            return atlasEdge;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to add edge", e);
        }
    }

    @Override
    public AtlasEdge<AtlasAgeVertex, AtlasAgeEdge> getEdgeBetweenVertices(AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> fromVertex,
                                                                           AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> toVertex,
                                                                           String relationshipLabel) {
        try {
            long fromId = ((AtlasAgeVertex) fromVertex).getWrappedVertex().getId();
            long toId   = ((AtlasAgeVertex) toVertex).getWrappedVertex().getId();
            String safeLabel = AgeCypherExecutor.escapeCypherLabel(relationshipLabel);

            String cypher = "MATCH (a:vertex)-[e:" + safeLabel + "]->(b:vertex) " +
                    "WHERE id(a) = " + fromId + " AND id(b) = " + toId + " RETURN id(e)";

            try (ResultSet rs = cypherExecutor.executeCypher(cypher)) {
                if (rs.next()) {
                    long edgeId = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                    return materializeEdge(edgeId, fromId, toId, relationshipLabel);  // load shadow props
                }
            }

            return null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get edge between vertices", e);
        }
    }

    @Override
    public AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> addVertex() {
        try {
            long vertexId = cypherExecutor.createVertexEmpty();

            AgeVertex ageVertex = new AgeVertex(vertexId);
            AtlasAgeVertex atlasVertex = new AtlasAgeVertex(this, ageVertex);

            cypherExecutor.syncVertexToShadow(vertexId, Collections.emptyMap());

            return atlasVertex;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to add vertex", e);
        }
    }

    @Override
    public void removeEdge(AtlasEdge<AtlasAgeVertex, AtlasAgeEdge> edge) {
        try {
            long edgeId = ((AtlasAgeEdge) edge).getWrappedEdge().getId();
            cypherExecutor.deleteEdge(edgeId);
            cypherExecutor.removeEdgeFromShadow(edgeId);
            ((AtlasAgeEdge) edge).getWrappedEdge().setRemoved(true);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to remove edge", e);
        }
    }

    @Override
    public void removeVertex(AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> vertex) {
        try {
            long vertexId = ((AtlasAgeVertex) vertex).getWrappedVertex().getId();
            cypherExecutor.deleteVertex(vertexId);
            cypherExecutor.removeVertexFromShadow(vertexId);
            ((AtlasAgeVertex) vertex).getWrappedVertex().setRemoved(true);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to remove vertex", e);
        }
    }

    @Override
    public AtlasEdge<AtlasAgeVertex, AtlasAgeEdge> getEdge(String edgeId) {
        try {
            long id = Long.parseLong(edgeId);
            String cypher = "MATCH (a:vertex)-[e]->(b:vertex) WHERE id(e) = " + id +
                    " RETURN id(e), id(a), id(b), label(e)";

            try (ResultSet rs = cypherExecutor.executeCypherWithTypes(cypher,
                    "eid agtype, aid agtype, bid agtype, lbl agtype")) {
                if (rs.next()) {
                    long eid = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                    long aid = AgeCypherExecutor.extractAgtypeId(rs.getString(2));
                    long bid = AgeCypherExecutor.extractAgtypeId(rs.getString(3));
                    String label = rs.getString(4);
                    if (label != null) {
                        label = label.replace("\"", "").trim();
                    }

                    AgeEdge ageEdge = new AgeEdge(eid, aid, bid, label);

                    // Load properties from shadow table
                    Map<String, Object> props = loadEdgePropertiesFromShadow(eid);
                    if (props != null) {
                        ageEdge.setProperties(props);
                    }

                    return new AtlasAgeEdge(this, ageEdge);
                }
            }

            return null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get edge " + edgeId, e);
        }
    }

    @Override
    public Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> getEdges() {
        try {
            List<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();
            String cypher = "MATCH (a:vertex)-[e]->(b:vertex) RETURN id(e), id(a), id(b), label(e)";

            try (ResultSet rs = cypherExecutor.executeCypherWithTypes(cypher,
                    "eid agtype, aid agtype, bid agtype, lbl agtype")) {
                while (rs.next()) {
                    long eid = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                    long aid = AgeCypherExecutor.extractAgtypeId(rs.getString(2));
                    long bid = AgeCypherExecutor.extractAgtypeId(rs.getString(3));
                    String label = rs.getString(4);
                    if (label != null) label = label.replace("\"", "").trim();

                    result.add(materializeEdge(eid, aid, bid, label));  // load shadow props
                }
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get edges", e);
        }
    }

    @Override
    public Iterable<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> getVertices() {
        try {
            List<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();
            String cypher = "MATCH (n:vertex) RETURN id(n)";

            try (ResultSet rs = cypherExecutor.executeCypher(cypher)) {
                while (rs.next()) {
                    long vid = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                    AgeVertex ageVertex = new AgeVertex(vid);

                    Map<String, Object> props = loadVertexPropertiesFromShadow(vid);
                    if (props != null) {
                        ageVertex.setProperties(props);
                    }

                    result.add(new AtlasAgeVertex(this, ageVertex));
                }
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get vertices", e);
        }
    }

    @Override
    public AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> getVertex(String vertexId) {
        try {
            long id = Long.parseLong(vertexId);

            // Resolve existence + properties from the indexed shadow table (PK on vertex_id) —
            // NOT a Cypher `MATCH (n:vertex) WHERE id(n)=X` check. AGE does not push id()
            // predicates to the index; it scans the whole vertex label (O(V) per call), which
            // is THE glossary-traversal bottleneck (getVertex is called per adjacent vertex, so
            // the cost is O(V) × edges ≈ O(V²)). The shadow row's presence IS the vertex's
            // existence. [aegir/signals AGE-backend fork — shadow-direct vertex load; upstreamable]
            Map<String, Object> props = loadVertexPropertiesFromShadow(id);
            if (props == null) {
                return null;
            }
            return new AtlasAgeVertex(this, new AgeVertex(id, props));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get vertex " + vertexId, e);
        }
    }

    /**
     * Build an AtlasAgeVertex for a known vertex id WITH its properties loaded from the
     * shadow table. Query paths (graph/vertex queries) MUST use this: a bare
     * AgeVertex(id) carries no properties, so __typeName/__state come back null and
     * entity retrieval, search resolution, and relationship-attribute mapping all NPE
     * ("No typename found" / getType(null)). [aegir/signals AGE-backend fork; upstreamable]
     */
    public AtlasVertex<AtlasAgeVertex, AtlasAgeEdge> materializeVertex(long id) {
        try {
            AgeVertex ageVertex = new AgeVertex(id);
            Map<String, Object> props = loadVertexPropertiesFromShadow(id);
            if (props != null) {
                ageVertex.setProperties(props);
            }
            return new AtlasAgeVertex(this, ageVertex);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to materialize vertex " + id, e);
        }
    }

    /**
     * Build an AtlasAgeEdge for a known edge id WITH its properties loaded from the shadow
     * table — the edge twin of materializeVertex. Traversal/query paths MUST use this, else
     * the edge's __typeName/__state come back null and relationship mapping NPEs
     * (mapEdgeToAtlasRelationship -> getType(null)). [aegir/signals AGE-backend fork; upstreamable]
     */
    public AtlasEdge<AtlasAgeVertex, AtlasAgeEdge> materializeEdge(long eid, long outVertexId, long inVertexId, String label) {
        try {
            Map<String, Object> props = loadEdgePropertiesFromShadow(eid);
            return new AtlasAgeEdge(this, new AgeEdge(eid, outVertexId, inVertexId, label, props));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to materialize edge " + eid, e);
        }
    }

    @Override
    public Set<String> getEdgeIndexKeys() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getVertexIndexKeys() {
        return Collections.emptySet();
    }

    @Override
    public Iterable<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> getVertices(String key, Object value) {
        try {
            List<AtlasVertex<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();
            String escapedKey = AgeCypherExecutor.escapeCypherKey(key);
            String cypherValue = AgeCypherExecutor.toCypherValue(value);

            String cypher = "MATCH (n:vertex) WHERE n." + escapedKey + " = " + cypherValue + " RETURN id(n)";

            List<Long> vertexIds = new ArrayList<>();
            try (ResultSet rs = cypherExecutor.executeCypher(cypher)) {
                while (rs.next()) {
                    vertexIds.add(AgeCypherExecutor.extractAgtypeId(rs.getString(1)));
                }
            }
            // batch-load matched-vertex properties in one query (was N+1)
            Map<Long, Map<String, Object>> propsByVid = loadVertexPropertiesBatch(vertexIds);
            for (Long vid : vertexIds) {
                AgeVertex ageVertex = new AgeVertex(vid);
                Map<String, Object> props = propsByVid.get(vid);
                if (props != null) {
                    ageVertex.setProperties(props);
                }
                result.add(new AtlasAgeVertex(this, ageVertex));
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get vertices by property", e);
        }
    }

    @Override
    public AtlasGraphQuery<AtlasAgeVertex, AtlasAgeEdge> query() {
        return new AtlasAgeGraphQuery(this);
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex<?, ?>, AtlasEdge<?, ?>> V(Object... vertexIds) {
        return new AtlasAgeGraphTraversal(this, vertexIds);
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex<?, ?>, AtlasEdge<?, ?>> E(Object... edgeIds) {
        return new AtlasAgeGraphTraversal(this, true, edgeIds);
    }

    @Override
    public AtlasIndexQuery<AtlasAgeVertex, AtlasAgeEdge> indexQuery(String indexName, String queryString) {
        return new AtlasAgeIndexQuery(this, indexName, queryString, 0);
    }

    @Override
    public AtlasIndexQuery<AtlasAgeVertex, AtlasAgeEdge> indexQuery(String indexName, String queryString, int offset) {
        return new AtlasAgeIndexQuery(this, indexName, queryString, offset);
    }

    @Override
    public AtlasIndexQuery<AtlasAgeVertex, AtlasAgeEdge> indexQuery(GraphIndexQueryParameters indexQueryParameters) {
        return new AtlasAgeIndexQuery(this,
                indexQueryParameters.getIndexName(),
                indexQueryParameters.getGraphQueryString(),
                indexQueryParameters.getOffset());
    }

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new AtlasAgeGraphManagement(this);
    }

    @Override
    public void commit() {
        txManager.commit();
    }

    @Override
    public void rollback() {
        txManager.rollback();
    }

    @Override
    public void shutdown() {
        txManager.releaseConnection();
        connectionPool.close();
    }

    @Override
    public void clear() {
        try {
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_fti_vertex");
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_fti_edge");
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_composite_index");
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_unique_key");
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_unique_type_key");
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_index_meta");

            String graphName = schemaManager.getGraphName();
            cypherExecutor.executeSqlUpdate(
                "SELECT * FROM ag_catalog.drop_graph('" + graphName + "', true)");
            cypherExecutor.executeSqlUpdate(
                "SELECT * FROM ag_catalog.create_graph('" + graphName + "')");

            commit();

            LOG.info("Graph cleared");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clear graph", e);
        }
    }

    @Override
    public Set<?> getOpenTransactions() {
        return txManager.hasActiveConnection() ? Collections.singleton("age-tx") : Collections.emptySet();
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {
        throw new UnsupportedOperationException("exportToGson not supported for AGE backend");
    }

    @Override
    public GroovyExpression generatePersisentToLogicalConversionExpression(GroovyExpression valueExpr, AtlasType type) {
        return valueExpr;
    }

    @Override
    public boolean isPropertyValueConversionNeeded(AtlasType type) {
        return false;
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {
        return GremlinVersion.THREE;
    }

    @Override
    public boolean requiresInitialIndexedPredicate() {
        return false;
    }

    @Override
    public GroovyExpression getInitialIndexedPredicate(GroovyExpression parent) {
        return parent;
    }

    @Override
    public GroovyExpression addOutputTransformationPredicate(GroovyExpression expr, boolean isSelect, boolean isPath) {
        return expr;
    }

    @Override
    public ScriptEngine getGremlinScriptEngine() throws AtlasBaseException {
        throw new AtlasBaseException("Gremlin script engine not supported for AGE backend. Use atlas.dsl.executor.traversal=true");
    }

    @Override
    public void releaseGremlinScriptEngine(ScriptEngine scriptEngine) {
        // no-op
    }

    @Override
    public Object executeGremlinScript(String query, boolean isPath) throws AtlasBaseException {
        throw new AtlasBaseException("Gremlin script execution not supported for AGE backend. Use atlas.dsl.executor.traversal=true");
    }

    @Override
    public Object executeGremlinScript(ScriptEngine scriptEngine, Map<? extends String, ? extends Object> bindings,
                                       String query, boolean isPath) throws ScriptException {
        throw new ScriptException("Gremlin script execution not supported for AGE backend");
    }

    @Override
    public boolean isMultiProperty(String name) {
        return multiProperties.contains(name);
    }

    @Override
    public AtlasIndexQueryParameter indexQueryParameter(String parameterName, String parameterValue) {
        return new AtlasAgeIndexQueryParameter(parameterName, parameterValue);
    }

    @Override
    public AtlasGraphIndexClient getGraphIndexClient() throws AtlasException {
        return new AtlasAgeGraphIndexClient(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<AtlasVertex> getAllEdgesVertices(AtlasVertex vertex) {
        List<AtlasVertex> result = new ArrayList<>();
        long vertexId = ((AtlasAgeVertex) vertex).getWrappedVertex().getId();

        try {
            String cypher = "MATCH (n:vertex)-[e]-(m:vertex) WHERE id(n) = " + vertexId + " RETURN DISTINCT id(m)";

            List<Long> vertexIds = new ArrayList<>();
            try (ResultSet rs = cypherExecutor.executeCypher(cypher)) {
                while (rs.next()) {
                    vertexIds.add(AgeCypherExecutor.extractAgtypeId(rs.getString(1)));
                }
            }
            // batch-load adjacent-vertex properties in one query (was N+1)
            Map<Long, Map<String, Object>> propsByVid = loadVertexPropertiesBatch(vertexIds);
            for (Long vid : vertexIds) {
                AgeVertex ageVertex = new AgeVertex(vid);
                Map<String, Object> props = propsByVid.get(vid);
                if (props != null) {
                    ageVertex.setProperties(props);
                }
                result.add(new AtlasAgeVertex(this, ageVertex));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get all edge vertices", e);
        }

        return result;
    }

    @Override
    public AtlasUniqueKeyHandler getUniqueKeyHandler() {
        return uniqueKeyHandler;
    }

    // Package-private methods used by other AGE classes

    AgeCypherExecutor getCypherExecutor() {
        return cypherExecutor;
    }

    AgeTransactionManager getTransactionManager() {
        return txManager;
    }

    void addMultiProperties(Set<String> properties) {
        multiProperties.addAll(properties);
    }

    Iterable<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> getEdgesForVertex(long vertexId,
                                                                         AtlasEdgeDirection direction,
                                                                         String edgeLabel) throws SQLException {
        List<AtlasEdge<AtlasAgeVertex, AtlasAgeEdge>> result = new ArrayList<>();

        String matchPattern;
        switch (direction) {
            case OUT:
                matchPattern = edgeLabel != null
                        ? "(a:vertex)-[e:" + AgeCypherExecutor.escapeCypherLabel(edgeLabel) + "]->(b:vertex)"
                        : "(a:vertex)-[e]->(b:vertex)";
                break;
            case IN:
                matchPattern = edgeLabel != null
                        ? "(b:vertex)-[e:" + AgeCypherExecutor.escapeCypherLabel(edgeLabel) + "]->(a:vertex)"
                        : "(b:vertex)-[e]->(a:vertex)";
                break;
            default: // BOTH
                matchPattern = edgeLabel != null
                        ? "(a:vertex)-[e:" + AgeCypherExecutor.escapeCypherLabel(edgeLabel) + "]-(b:vertex)"
                        : "(a:vertex)-[e]-(b:vertex)";
                break;
        }

        String cypher = "MATCH " + matchPattern + " WHERE id(a) = " + vertexId +
                " RETURN id(e), id(startNode(e)), id(endNode(e)), label(e)";

        List<AgeEdge> drained = new ArrayList<>();
        List<Long> edgeIds = new ArrayList<>();
        try (ResultSet rs = cypherExecutor.executeCypherWithTypes(cypher,
                "eid agtype, sid agtype, eid2 agtype, lbl agtype")) {
            while (rs.next()) {
                long eid = AgeCypherExecutor.extractAgtypeId(rs.getString(1));
                long sid = AgeCypherExecutor.extractAgtypeId(rs.getString(2));
                long tid = AgeCypherExecutor.extractAgtypeId(rs.getString(3));
                String label = rs.getString(4);
                if (label != null) label = label.replace("\"", "").trim();

                drained.add(new AgeEdge(eid, sid, tid, label));
                edgeIds.add(eid);
            }
        }

        // batch-load ALL edge properties in one query (was N+1: a shadow query per edge)
        Map<Long, Map<String, Object>> propsByEid = loadEdgePropertiesBatch(edgeIds);
        for (AgeEdge ageEdge : drained) {
            Map<String, Object> props = propsByEid.get(ageEdge.getId());
            if (props != null) {
                ageEdge.setProperties(props);
            }
            result.add(new AtlasAgeEdge(this, ageEdge));
        }

        return result;
    }

    long getEdgesCountForVertex(long vertexId, AtlasEdgeDirection direction, String edgeLabel) throws SQLException {
        String matchPattern;
        switch (direction) {
            case OUT:
                matchPattern = edgeLabel != null
                        ? "(a:vertex)-[e:" + AgeCypherExecutor.escapeCypherLabel(edgeLabel) + "]->(b:vertex)"
                        : "(a:vertex)-[e]->(b:vertex)";
                break;
            case IN:
                matchPattern = edgeLabel != null
                        ? "(b:vertex)-[e:" + AgeCypherExecutor.escapeCypherLabel(edgeLabel) + "]->(a:vertex)"
                        : "(b:vertex)-[e]->(a:vertex)";
                break;
            default:
                matchPattern = edgeLabel != null
                        ? "(a:vertex)-[e:" + AgeCypherExecutor.escapeCypherLabel(edgeLabel) + "]-(b:vertex)"
                        : "(a:vertex)-[e]-(b:vertex)";
                break;
        }

        String cypher = "MATCH " + matchPattern + " WHERE id(a) = " + vertexId + " RETURN count(e)";

        try (ResultSet rs = cypherExecutor.executeCypher(cypher)) {
            if (rs.next()) {
                return AgeCypherExecutor.extractAgtypeId(rs.getString(1));
            }
        }

        return 0;
    }

    private Map<String, Object> loadVertexPropertiesFromShadow(long vertexId) throws SQLException {
        String sql = "SELECT properties FROM atlas_fti_vertex WHERE vertex_id = " + vertexId;

        try (ResultSet rs = cypherExecutor.executeSql(sql)) {
            if (rs.next()) {
                String jsonb = rs.getString(1);
                if (jsonb != null) {
                    return AgeCypherExecutor.parseAgtypeProperties(jsonb);
                }
            }
        }

        return null;
    }

    private Map<String, Object> loadEdgePropertiesFromShadow(long edgeId) throws SQLException {
        String sql = "SELECT properties FROM atlas_fti_edge WHERE edge_id = " + edgeId;

        try (ResultSet rs = cypherExecutor.executeSql(sql)) {
            if (rs.next()) {
                String jsonb = rs.getString(1);
                if (jsonb != null) {
                    return AgeCypherExecutor.parseAgtypeProperties(jsonb);
                }
            }
        }

        return null;
    }

    /**
     * Load the shadow-table properties for MANY vertices in a SINGLE query (id -> props).
     * Replaces the N+1 pattern of calling loadVertexPropertiesFromShadow per vertex inside a
     * traversal/scan loop — the dominant cause of multi-second bulk hangs on the AGE backend.
     * [aegir/signals AGE-backend fork — batched materialization; upstreamable]
     */
    private Map<Long, Map<String, Object>> loadVertexPropertiesBatch(List<Long> vertexIds) throws SQLException {
        Map<Long, Map<String, Object>> out = new HashMap<>();
        if (vertexIds == null || vertexIds.isEmpty()) {
            return out;
        }
        StringBuilder ids = new StringBuilder();
        for (int i = 0; i < vertexIds.size(); i++) {
            if (i > 0) ids.append(",");
            ids.append(vertexIds.get(i));
        }
        String sql = "SELECT vertex_id, properties FROM atlas_fti_vertex WHERE vertex_id = ANY(ARRAY[" + ids + "]::bigint[])";
        try (ResultSet rs = cypherExecutor.executeSql(sql)) {
            while (rs.next()) {
                String jsonb = rs.getString(2);
                if (jsonb != null) {
                    out.put(rs.getLong(1), AgeCypherExecutor.parseAgtypeProperties(jsonb));
                }
            }
        }
        return out;
    }

    /** Edge twin of {@link #loadVertexPropertiesBatch} — one query for many edges' properties. */
    private Map<Long, Map<String, Object>> loadEdgePropertiesBatch(List<Long> edgeIds) throws SQLException {
        Map<Long, Map<String, Object>> out = new HashMap<>();
        if (edgeIds == null || edgeIds.isEmpty()) {
            return out;
        }
        StringBuilder ids = new StringBuilder();
        for (int i = 0; i < edgeIds.size(); i++) {
            if (i > 0) ids.append(",");
            ids.append(edgeIds.get(i));
        }
        String sql = "SELECT edge_id, properties FROM atlas_fti_edge WHERE edge_id = ANY(ARRAY[" + ids + "]::bigint[])";
        try (ResultSet rs = cypherExecutor.executeSql(sql)) {
            while (rs.next()) {
                String jsonb = rs.getString(2);
                if (jsonb != null) {
                    out.put(rs.getLong(1), AgeCypherExecutor.parseAgtypeProperties(jsonb));
                }
            }
        }
        return out;
    }
}
