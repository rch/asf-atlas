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

import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AtlasAgeGraphIndexClient implements AtlasGraphIndexClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAgeGraphIndexClient.class);

    private final AtlasAgeGraph graph;

    public AtlasAgeGraphIndexClient(AtlasAgeGraph graph) {
        this.graph = graph;
    }

    @Override
    public Map<String, List<org.apache.atlas.model.discovery.AtlasAggregationEntry>> getAggregatedMetrics(AggregationContext aggregationContext) {
        // Implement aggregation using GROUP BY on shadow table
        Map<String, List<org.apache.atlas.model.discovery.AtlasAggregationEntry>> result = new HashMap<>();

        if (aggregationContext == null) {
            return result;
        }

        // For each aggregation field, compute counts
        Set<String> aggregationFieldNames = aggregationContext.getAggregationFieldNames();
        if (aggregationFieldNames == null) {
            return result;
        }

        for (String fieldName : aggregationFieldNames) {

            try {
                String sql = "SELECT properties->>'" + fieldName.replace("'", "''") +
                        "' AS val, count(*) AS cnt FROM atlas_fti_vertex " +
                        "WHERE properties->>'" + fieldName.replace("'", "''") + "' IS NOT NULL " +
                        "GROUP BY val ORDER BY cnt DESC LIMIT 100";

                List<org.apache.atlas.model.discovery.AtlasAggregationEntry> entries = new ArrayList<>();

                try (ResultSet rs = graph.getCypherExecutor().executeSql(sql)) {
                    while (rs.next()) {
                        String val = rs.getString(1);
                        long count = rs.getLong(2);
                        entries.add(new org.apache.atlas.model.discovery.AtlasAggregationEntry(val, count));
                    }
                }

                result.put(fieldName, entries);
            } catch (SQLException e) {
                LOG.warn("Failed to compute aggregation for field: {}", fieldName, e);
                result.put(fieldName, Collections.emptyList());
            }
        }

        return result;
    }

    @Override
    public List<String> getSuggestions(String prefixString, String indexFieldName) {
        try {
            List<String> suggestions = new ArrayList<>();

            String sql = "SELECT DISTINCT properties->>'" + indexFieldName.replace("'", "''") +
                    "' AS val FROM atlas_fti_vertex " +
                    "WHERE properties->>'" + indexFieldName.replace("'", "''") +
                    "' % '" + prefixString.replace("'", "''") + "' " +
                    "ORDER BY similarity(properties->>'" + indexFieldName.replace("'", "''") +
                    "', '" + prefixString.replace("'", "''") + "') DESC LIMIT 10";

            try (ResultSet rs = graph.getCypherExecutor().executeSql(sql)) {
                while (rs.next()) {
                    String val = rs.getString(1);
                    if (val != null) {
                        suggestions.add(val);
                    }
                }
            }

            return suggestions;
        } catch (SQLException e) {
            LOG.warn("Failed to get suggestions for '{}' on field '{}'", prefixString, indexFieldName, e);
            return Collections.emptyList();
        }
    }

    @Override
    public void applySearchWeight(String collectionName, Map<String, Integer> indexFieldName2SearchWeightMap) {
        // PostgreSQL tsvector weights could be used here (A, B, C, D weights)
        // For now, all fields are weighted equally
    }

    @Override
    public void applySuggestionFields(String collectionName, List<String> suggestionProperties) {
        // pg_trgm handles suggestions automatically based on the indexed column
    }

    @Override
    public boolean isHealthy() {
        try {
            try (ResultSet rs = graph.getCypherExecutor().executeSql("SELECT 1")) {
                return rs.next();
            }
        } catch (SQLException e) {
            return false;
        }
    }
}
