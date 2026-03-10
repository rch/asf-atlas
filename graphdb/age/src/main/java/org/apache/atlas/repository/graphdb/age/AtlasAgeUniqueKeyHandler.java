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

import org.apache.atlas.repository.graphdb.AtlasUniqueKeyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class AtlasAgeUniqueKeyHandler extends AtlasUniqueKeyHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAgeUniqueKeyHandler.class);

    private final AgeCypherExecutor cypherExecutor;

    public AtlasAgeUniqueKeyHandler(AgeCypherExecutor cypherExecutor) {
        this.cypherExecutor = cypherExecutor;
    }

    @Override
    public void addUniqueKey(String keyName, Object value, Object elementId, boolean isVertex) {
        try {
            String sql = "INSERT INTO atlas_unique_key (key_name, key_value, element_id, is_vertex) " +
                    "VALUES ('" + escape(keyName) + "', '" + escape(String.valueOf(value)) + "', " +
                    Long.parseLong(elementId.toString()) + ", " + isVertex + ") " +
                    "ON CONFLICT (key_name, key_value) DO UPDATE SET element_id = " +
                    Long.parseLong(elementId.toString()) + ", is_vertex = " + isVertex;
            cypherExecutor.executeSqlUpdate(sql);
        } catch (SQLException e) {
            LOG.error("Failed to add unique key: {}", keyName, e);
            throw new RuntimeException("Failed to add unique key", e);
        }
    }

    @Override
    public void removeUniqueKey(String keyName, Object value, boolean isVertex) {
        try {
            String sql = "DELETE FROM atlas_unique_key WHERE key_name = '" + escape(keyName) +
                    "' AND key_value = '" + escape(String.valueOf(value)) + "'";
            cypherExecutor.executeSqlUpdate(sql);
        } catch (SQLException e) {
            LOG.error("Failed to remove unique key: {}", keyName, e);
            throw new RuntimeException("Failed to remove unique key", e);
        }
    }

    @Override
    public void addTypeUniqueKey(String typeName, String keyName, Object value, Object elementId, boolean isVertex) {
        try {
            String sql = "INSERT INTO atlas_unique_type_key (type_name, key_name, key_value, element_id, is_vertex) " +
                    "VALUES ('" + escape(typeName) + "', '" + escape(keyName) + "', '" +
                    escape(String.valueOf(value)) + "', " +
                    Long.parseLong(elementId.toString()) + ", " + isVertex + ") " +
                    "ON CONFLICT (type_name, key_name, key_value) DO UPDATE SET element_id = " +
                    Long.parseLong(elementId.toString()) + ", is_vertex = " + isVertex;
            cypherExecutor.executeSqlUpdate(sql);
        } catch (SQLException e) {
            LOG.error("Failed to add type unique key: {}.{}", typeName, keyName, e);
            throw new RuntimeException("Failed to add type unique key", e);
        }
    }

    @Override
    public void removeTypeUniqueKey(String typeName, String keyName, Object value, boolean isVertex) {
        try {
            String sql = "DELETE FROM atlas_unique_type_key WHERE type_name = '" + escape(typeName) +
                    "' AND key_name = '" + escape(keyName) +
                    "' AND key_value = '" + escape(String.valueOf(value)) + "'";
            cypherExecutor.executeSqlUpdate(sql);
        } catch (SQLException e) {
            LOG.error("Failed to remove type unique key: {}.{}", typeName, keyName, e);
            throw new RuntimeException("Failed to remove type unique key", e);
        }
    }

    @Override
    public void removeUniqueKeysForVertexId(Object vertexId) {
        try {
            long id = Long.parseLong(vertexId.toString());
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_unique_key WHERE element_id = " + id + " AND is_vertex = true");
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_unique_type_key WHERE element_id = " + id + " AND is_vertex = true");
        } catch (SQLException e) {
            LOG.error("Failed to remove unique keys for vertex: {}", vertexId, e);
            throw new RuntimeException("Failed to remove unique keys for vertex", e);
        }
    }

    @Override
    public void removeUniqueKeysForEdgeId(Object edgeId) {
        try {
            long id = Long.parseLong(edgeId.toString());
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_unique_key WHERE element_id = " + id + " AND is_vertex = false");
            cypherExecutor.executeSqlUpdate("DELETE FROM atlas_unique_type_key WHERE element_id = " + id + " AND is_vertex = false");
        } catch (SQLException e) {
            LOG.error("Failed to remove unique keys for edge: {}", edgeId, e);
            throw new RuntimeException("Failed to remove unique keys for edge", e);
        }
    }

    private String escape(String value) {
        return value.replace("'", "''");
    }
}
