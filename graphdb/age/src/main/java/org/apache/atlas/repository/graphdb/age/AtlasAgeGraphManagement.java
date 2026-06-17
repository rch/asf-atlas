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
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasEdgeLabel;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AtlasAgeGraphManagement implements AtlasGraphManagement {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAgeGraphManagement.class);

    private final AtlasAgeGraph graph;
    private final Map<String, AtlasAgePropertyKey> propertyKeys = new HashMap<>();
    private final Map<String, AtlasAgeEdgeLabel> edgeLabels = new HashMap<>();
    private final Map<String, AtlasAgeGraphIndex> indexes = new HashMap<>();
    private final Set<String> newMultiProperties = new HashSet<>();
    private boolean isSuccess;

    public AtlasAgeGraphManagement(AtlasAgeGraph graph) {
        this.graph = graph;
        loadExistingMetadata();
    }

    @Override
    public boolean containsPropertyKey(String propertyName) {
        return propertyKeys.containsKey(propertyName);
    }

    @Override
    public AtlasPropertyKey makePropertyKey(String propertyName, Class<?> propertyClass, AtlasCardinality cardinality) {
        if (cardinality.isMany()) {
            newMultiProperties.add(propertyName);
        }

        AtlasAgePropertyKey key = new AtlasAgePropertyKey(propertyName, cardinality);
        propertyKeys.put(propertyName, key);
        return key;
    }

    @Override
    public AtlasEdgeLabel makeEdgeLabel(String label) {
        AtlasAgeEdgeLabel edgeLabel = new AtlasAgeEdgeLabel(label);
        edgeLabels.put(label, edgeLabel);
        return edgeLabel;
    }

    @Override
    public void deletePropertyKey(String propertyKey) {
        propertyKeys.remove(propertyKey);
    }

    @Override
    public AtlasPropertyKey getPropertyKey(String propertyName) {
        return propertyKeys.get(propertyName);
    }

    @Override
    public AtlasEdgeLabel getEdgeLabel(String label) {
        return edgeLabels.get(label);
    }

    @Override
    public void createVertexCompositeIndex(String indexName, boolean isUnique, List<AtlasPropertyKey> propertyKeys) {
        createCompositeIndex(indexName, isUnique, propertyKeys, true);
    }

    @Override
    public void createEdgeCompositeIndex(String indexName, boolean isUnique, List<AtlasPropertyKey> propertyKeys) {
        createCompositeIndex(indexName, isUnique, propertyKeys, false);
    }

    @Override
    public AtlasGraphIndex getGraphIndex(String indexName) {
        return indexes.get(indexName);
    }

    @Override
    public boolean edgeIndexExist(String label, String indexName) {
        return indexes.containsKey(indexName);
    }

    @Override
    public void createVertexMixedIndex(String indexName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        storeIndexMeta(indexName, "vertex", "mixed", false, propertyKeys);
        indexes.put(indexName, new AtlasAgeGraphIndex(indexName, false, true, false, new HashSet<>(propertyKeys)));
    }

    @Override
    public void createEdgeMixedIndex(String indexName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        storeIndexMeta(indexName, "edge", "mixed", false, propertyKeys);
        indexes.put(indexName, new AtlasAgeGraphIndex(indexName, false, false, false, new HashSet<>(propertyKeys)));
    }

    @Override
    public void createEdgeIndex(String label, String indexName, AtlasEdgeDirection edgeDirection, List<AtlasPropertyKey> propertyKeys) {
        storeIndexMeta(indexName, "edge", "composite", false, propertyKeys);
        createPropertyIndex(indexName, propertyKeys, false);
        indexes.put(indexName, new AtlasAgeGraphIndex(indexName, true, false, false, new HashSet<>(propertyKeys)));
    }

    @Override
    public void createFullTextMixedIndex(String indexName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        storeIndexMeta(indexName, "vertex", "fulltext", false, propertyKeys);
        updateTriggerForFields(propertyKeys);
        indexes.put(indexName, new AtlasAgeGraphIndex(indexName, false, true, false, new HashSet<>(propertyKeys)));
    }

    @Override
    public String addMixedIndex(String indexName, AtlasPropertyKey propertyKey, boolean isStringField) {
        AtlasAgeGraphIndex idx = (AtlasAgeGraphIndex) indexes.get(indexName);
        if (idx != null) {
            idx.addFieldKey(propertyKey);
        }
        return propertyKey.getName();
    }

    @Override
    public String getIndexFieldName(String indexName, AtlasPropertyKey propertyKey, boolean isStringField) {
        return propertyKey.getName();
    }

    @Override
    public void updateUniqueIndexesForConsistencyLock() {
        // PostgreSQL handles consistency via MVCC — no additional locking needed
    }

    @Override
    public void updateSchemaStatus() {
        // PostgreSQL indexes are always immediately available
    }

    @Override
    public void reindex(String indexName, List<AtlasElement> elements) throws Exception {
        // PostgreSQL tsvector triggers auto-update on insert/update — no manual reindex needed
    }

    @Override
    public Object startIndexRecovery(long startTime) {
        // PostgreSQL indexes are transactionally consistent — no recovery needed
        return null;
    }

    @Override
    public void stopIndexRecovery(Object txRecoveryObject) {
        // no-op
    }

    @Override
    public void printIndexRecoveryStats(Object txRecoveryObject) {
        // no-op
    }

    @Override
    public void setIsSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    @Override
    public void close() throws Exception {
        if (isSuccess) {
            graph.addMultiProperties(newMultiProperties);
            newMultiProperties.clear();
            graph.commit();
        } else {
            graph.rollback();
        }
    }

    private void createCompositeIndex(String indexName, boolean isUnique, List<AtlasPropertyKey> propertyKeys, boolean isVertex) {
        storeIndexMeta(indexName, isVertex ? "vertex" : "edge", "composite", isUnique, propertyKeys);
        createPropertyIndex(indexName, propertyKeys, isVertex);
        indexes.put(indexName, new AtlasAgeGraphIndex(indexName, true, isVertex, isUnique, new HashSet<>(propertyKeys)));
    }

    private void storeIndexMeta(String indexName, String elementType, String indexType, boolean isUnique, List<AtlasPropertyKey> keys) {
        try {
            StringBuilder fieldKeys = new StringBuilder("[");
            boolean first = true;
            for (AtlasPropertyKey key : keys) {
                if (!first) fieldKeys.append(",");
                fieldKeys.append("\"").append(key.getName()).append("\"");
                first = false;
            }
            fieldKeys.append("]");

            String sql = "INSERT INTO atlas_index_meta (index_name, element_type, index_type, is_unique, field_keys) " +
                    "VALUES ('" + AgeCypherExecutor.escapeCypherString(indexName) + "', '" + elementType + "', '" + indexType + "', " +
                    isUnique + ", '" + fieldKeys + "'::jsonb) " +
                    "ON CONFLICT (index_name) DO UPDATE SET field_keys = '" + fieldKeys + "'::jsonb";
            graph.getCypherExecutor().executeSqlUpdate(sql);
        } catch (SQLException e) {
            LOG.warn("Failed to store index metadata for {}", indexName, e);
        }
    }

    /**
     * Create a REAL Postgres B-tree index on the shadow table's JSONB-extracted property keys, so
     * type / qualifiedName predicate filters use an index instead of a sequential scan. Non-unique
     * (uniqueness is enforced separately via atlas_unique_key) — a pure query-performance index that
     * completes the previously metadata-only index path. [aegir/signals AGE-backend fork; upstreamable]
     */
    private void createPropertyIndex(String indexName, List<AtlasPropertyKey> keys, boolean isVertex) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        try {
            String        table = isVertex ? "atlas_fti_vertex" : "atlas_fti_edge";
            String        pgName = pgIndexName(indexName);
            StringBuilder cols   = new StringBuilder();
            for (AtlasPropertyKey key : keys) {
                if (cols.length() > 0) {
                    cols.append(", ");
                }
                cols.append("(properties->>'")
                    .append(AgeCypherExecutor.escapeCypherString(key.getName()))
                    .append("')");
            }
            String sql = "CREATE INDEX IF NOT EXISTS " + pgName + " ON " + table + " USING BTREE (" + cols + ")";
            graph.getCypherExecutor().executeSqlUpdate(sql);
            LOG.info("AGE: created Postgres property index {} on {} ({})", pgName, table, cols);
        } catch (SQLException e) {
            LOG.warn("AGE: failed to create Postgres property index for {}", indexName, e);
        }
    }

    /** A valid, collision-resistant Postgres identifier for an Atlas index name (<=63 chars). */
    private static String pgIndexName(String indexName) {
        String s = ("idx_prop_" + indexName).toLowerCase().replaceAll("[^a-z0-9_]", "_");
        return s.length() > 63 ? s.substring(0, 63) : s;
    }

    private void updateTriggerForFields(List<AtlasPropertyKey> propertyKeys) {
        // The trigger function already covers the main Atlas fields.
        // Additional fields could be added dynamically by recreating the trigger,
        // but the default set covers the primary use cases.
    }

    private void loadExistingMetadata() {
        try {
            // Load property keys from index metadata
            String sql = "SELECT index_name, element_type, index_type, is_unique, field_keys FROM atlas_index_meta";
            try (java.sql.ResultSet rs = graph.getCypherExecutor().executeSql(sql)) {
                while (rs.next()) {
                    String indexName = rs.getString(1);
                    String elementType = rs.getString(2);
                    boolean isUnique = rs.getBoolean(4);
                    boolean isVertex = "vertex".equals(elementType);

                    indexes.put(indexName, new AtlasAgeGraphIndex(indexName, true, isVertex, isUnique, new HashSet<>()));
                }
            }
        } catch (SQLException e) {
            LOG.debug("No existing index metadata found (table may not exist yet)");
        }
    }
}
