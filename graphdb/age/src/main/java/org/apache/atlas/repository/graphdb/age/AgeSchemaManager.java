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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AgeSchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(AgeSchemaManager.class);

    private final AgeConnectionPool connectionPool;
    private final String graphName;

    public AgeSchemaManager(AgeConnectionPool connectionPool, String graphName) {
        this.connectionPool = connectionPool;
        this.graphName = graphName;
    }

    public void initializeSchema() {
        try (Connection conn = connectionPool.getConnection()) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                initializeExtensions(stmt);
                initializeGraph(stmt);
                initializeShadowTables(stmt);
                initializeIndexes(stmt);
                initializeTriggers(stmt);
                initializeMetaTables(stmt);
            }
            LOG.info("AGE schema initialized for graph '{}'", graphName);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize AGE schema", e);
        }
    }

    private void initializeExtensions(Statement stmt) throws SQLException {
        stmt.execute("CREATE EXTENSION IF NOT EXISTS age");
        stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        stmt.execute("LOAD 'age'");
        stmt.execute("SET search_path = ag_catalog, \"$user\", public");
    }

    private void initializeGraph(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery(
                "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = '" + graphName + "'");
        rs.next();
        if (rs.getInt(1) == 0) {
            stmt.execute("SELECT * FROM ag_catalog.create_graph('" + graphName + "')");
            LOG.info("Created AGE graph '{}'", graphName);
        } else {
            LOG.info("AGE graph '{}' already exists", graphName);
        }
    }

    private void initializeShadowTables(Statement stmt) throws SQLException {
        stmt.execute(
            "CREATE TABLE IF NOT EXISTS atlas_fti_vertex (" +
            "    vertex_id   bigint PRIMARY KEY," +
            "    properties  jsonb NOT NULL DEFAULT '{}'," +
            "    search_text tsvector," +
            "    modified_at timestamptz DEFAULT now()" +
            ")");

        stmt.execute(
            "CREATE TABLE IF NOT EXISTS atlas_fti_edge (" +
            "    edge_id     bigint PRIMARY KEY," +
            "    properties  jsonb NOT NULL DEFAULT '{}'," +
            "    search_text tsvector," +
            "    modified_at timestamptz DEFAULT now()" +
            ")");

        stmt.execute(
            "CREATE TABLE IF NOT EXISTS atlas_composite_index (" +
            "    index_name     text NOT NULL," +
            "    property_key   text NOT NULL," +
            "    property_value text NOT NULL," +
            "    element_id     bigint NOT NULL," +
            "    is_vertex      boolean NOT NULL," +
            "    PRIMARY KEY (index_name, property_key, property_value, element_id)" +
            ")");

        stmt.execute(
            "CREATE TABLE IF NOT EXISTS atlas_unique_key (" +
            "    key_name   text NOT NULL," +
            "    key_value  text NOT NULL," +
            "    element_id bigint NOT NULL," +
            "    is_vertex  boolean NOT NULL," +
            "    PRIMARY KEY (key_name, key_value)" +
            ")");

        stmt.execute(
            "CREATE TABLE IF NOT EXISTS atlas_unique_type_key (" +
            "    type_name  text NOT NULL," +
            "    key_name   text NOT NULL," +
            "    key_value  text NOT NULL," +
            "    element_id bigint NOT NULL," +
            "    is_vertex  boolean NOT NULL," +
            "    PRIMARY KEY (type_name, key_name, key_value)" +
            ")");
    }

    private void initializeIndexes(Statement stmt) throws SQLException {
        executeIgnoreExisting(stmt,
            "CREATE INDEX idx_vertex_fti ON atlas_fti_vertex USING GIN(search_text)");
        executeIgnoreExisting(stmt,
            "CREATE INDEX idx_edge_fti ON atlas_fti_edge USING GIN(search_text)");
        executeIgnoreExisting(stmt,
            "CREATE INDEX idx_vertex_props ON atlas_fti_vertex USING GIN(properties jsonb_path_ops)");
        executeIgnoreExisting(stmt,
            "CREATE INDEX idx_edge_props ON atlas_fti_edge USING GIN(properties jsonb_path_ops)");
        executeIgnoreExisting(stmt,
            "CREATE INDEX idx_composite_element ON atlas_composite_index (element_id)");
    }

    private void initializeTriggers(Statement stmt) throws SQLException {
        stmt.execute(
            "CREATE OR REPLACE FUNCTION update_vertex_search_text() RETURNS TRIGGER AS $$\n" +
            "BEGIN\n" +
            "    NEW.search_text := to_tsvector('english',\n" +
            "        coalesce(NEW.properties->>'__typeName', '') || ' ' ||\n" +
            "        coalesce(NEW.properties->>'Referenceable.qualifiedName', '') || ' ' ||\n" +
            "        coalesce(NEW.properties->>'Asset.name', '') || ' ' ||\n" +
            "        coalesce(NEW.properties->>'Asset.description', '') || ' ' ||\n" +
            "        coalesce(NEW.properties->>'__classificationNames', '') || ' ' ||\n" +
            "        coalesce(NEW.properties->>'__fullText', ''));\n" +
            "    NEW.modified_at := now();\n" +
            "    RETURN NEW;\n" +
            "END; $$ LANGUAGE plpgsql");

        stmt.execute("DROP TRIGGER IF EXISTS trg_vertex_search ON atlas_fti_vertex");
        stmt.execute(
            "CREATE TRIGGER trg_vertex_search BEFORE INSERT OR UPDATE " +
            "ON atlas_fti_vertex FOR EACH ROW EXECUTE FUNCTION update_vertex_search_text()");

        stmt.execute(
            "CREATE OR REPLACE FUNCTION update_edge_search_text() RETURNS TRIGGER AS $$\n" +
            "BEGIN\n" +
            "    NEW.search_text := to_tsvector('english',\n" +
            "        coalesce(NEW.properties->>'__typeName', '') || ' ' ||\n" +
            "        coalesce(NEW.properties->>'__label', ''));\n" +
            "    NEW.modified_at := now();\n" +
            "    RETURN NEW;\n" +
            "END; $$ LANGUAGE plpgsql");

        stmt.execute("DROP TRIGGER IF EXISTS trg_edge_search ON atlas_fti_edge");
        stmt.execute(
            "CREATE TRIGGER trg_edge_search BEFORE INSERT OR UPDATE " +
            "ON atlas_fti_edge FOR EACH ROW EXECUTE FUNCTION update_edge_search_text()");
    }

    private void initializeMetaTables(Statement stmt) throws SQLException {
        stmt.execute(
            "CREATE TABLE IF NOT EXISTS atlas_index_meta (" +
            "    index_name   text PRIMARY KEY," +
            "    element_type text NOT NULL," +
            "    index_type   text NOT NULL," +
            "    is_unique    boolean DEFAULT false," +
            "    field_keys   jsonb DEFAULT '[]'" +
            ")");
    }

    private void executeIgnoreExisting(Statement stmt, String sql) throws SQLException {
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            if (!e.getMessage().contains("already exists")) {
                throw e;
            }
        }
    }

    public String getGraphName() {
        return graphName;
    }
}
