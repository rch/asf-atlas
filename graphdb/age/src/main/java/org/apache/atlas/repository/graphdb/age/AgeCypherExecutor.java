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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AgeCypherExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(AgeCypherExecutor.class);

    private final AgeTransactionManager txManager;
    private final String graphName;

    public AgeCypherExecutor(AgeTransactionManager txManager, String graphName) {
        this.txManager = txManager;
        this.graphName = graphName;
    }

    public ResultSet executeCypher(String cypher) throws SQLException {
        Connection conn = txManager.getConnection();

        ensureAgeLoaded(conn);

        String sql = "SELECT * FROM cypher('" + graphName + "', $$ " + cypher + " $$) AS (result agtype)";

        LOG.debug("Executing Cypher: {}", cypher);

        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql);
    }

    public ResultSet executeCypherWithTypes(String cypher, String resultSpec) throws SQLException {
        Connection conn = txManager.getConnection();

        ensureAgeLoaded(conn);

        String sql = "SELECT * FROM cypher('" + graphName + "', $$ " + cypher + " $$) AS (" + resultSpec + ")";

        LOG.debug("Executing Cypher: {} [resultSpec: {}]", cypher, resultSpec);

        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql);
    }

    public void executeCypherUpdate(String cypher) throws SQLException {
        Connection conn = txManager.getConnection();

        ensureAgeLoaded(conn);

        String sql = "SELECT * FROM cypher('" + graphName + "', $$ " + cypher + " $$) AS (result agtype)";

        LOG.debug("Executing Cypher update: {}", cypher);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    public ResultSet executeSql(String sql) throws SQLException {
        Connection conn = txManager.getConnection();
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql);
    }

    public void executeSqlUpdate(String sql) throws SQLException {
        Connection conn = txManager.getConnection();

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    public long createVertex(Map<String, Object> properties) throws SQLException {
        StringBuilder propsBuilder = new StringBuilder("{");
        boolean first = true;

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (!first) propsBuilder.append(", ");
            propsBuilder.append(escapeCypherKey(entry.getKey()));
            propsBuilder.append(": ");
            propsBuilder.append(toCypherValue(entry.getValue()));
            first = false;
        }

        propsBuilder.append("}");

        String cypher = "CREATE (n:vertex " + propsBuilder + ") RETURN id(n)";

        try (ResultSet rs = executeCypher(cypher)) {
            if (rs.next()) {
                return extractAgtypeId(rs.getString(1));
            }
        }

        throw new SQLException("Failed to create vertex");
    }

    public long createVertexEmpty() throws SQLException {
        String cypher = "CREATE (n:vertex) RETURN id(n)";

        try (ResultSet rs = executeCypher(cypher)) {
            if (rs.next()) {
                return extractAgtypeId(rs.getString(1));
            }
        }

        throw new SQLException("Failed to create vertex");
    }

    public long createEdge(long outVertexId, long inVertexId, String label) throws SQLException {
        String safeLabel = escapeCypherLabel(label);
        String cypher = "MATCH (a:vertex), (b:vertex) WHERE id(a) = " + outVertexId +
                " AND id(b) = " + inVertexId +
                " CREATE (a)-[e:" + safeLabel + "]->(b) RETURN id(e)";

        try (ResultSet rs = executeCypher(cypher)) {
            if (rs.next()) {
                return extractAgtypeId(rs.getString(1));
            }
        }

        throw new SQLException("Failed to create edge from " + outVertexId + " to " + inVertexId + " with label " + label);
    }

    public void setVertexProperty(long vertexId, String key, Object value) throws SQLException {
        String cypher = "MATCH (n:vertex) WHERE id(n) = " + vertexId +
                " SET n." + escapeCypherKey(key) + " = " + toCypherValue(value) +
                " RETURN n";
        executeCypherUpdate(cypher);
    }

    public void removeVertexProperty(long vertexId, String key) throws SQLException {
        String cypher = "MATCH (n:vertex) WHERE id(n) = " + vertexId +
                " SET n." + escapeCypherKey(key) + " = NULL RETURN n";
        executeCypherUpdate(cypher);
    }

    public void setEdgeProperty(long edgeId, String key, Object value) throws SQLException {
        String cypher = "MATCH ()-[e]->() WHERE id(e) = " + edgeId +
                " SET e." + escapeCypherKey(key) + " = " + toCypherValue(value) +
                " RETURN e";
        executeCypherUpdate(cypher);
    }

    public void removeEdgeProperty(long edgeId, String key) throws SQLException {
        String cypher = "MATCH ()-[e]->() WHERE id(e) = " + edgeId +
                " SET e." + escapeCypherKey(key) + " = NULL RETURN e";
        executeCypherUpdate(cypher);
    }

    public void deleteVertex(long vertexId) throws SQLException {
        String cypher = "MATCH (n:vertex) WHERE id(n) = " + vertexId + " DETACH DELETE n";
        executeCypherUpdate(cypher);
    }

    public void deleteEdge(long edgeId) throws SQLException {
        String cypher = "MATCH ()-[e]->() WHERE id(e) = " + edgeId + " DELETE e";
        executeCypherUpdate(cypher);
    }

    public Map<String, Object> getVertexProperties(long vertexId) throws SQLException {
        String cypher = "MATCH (n:vertex) WHERE id(n) = " + vertexId + " RETURN properties(n)";

        try (ResultSet rs = executeCypher(cypher)) {
            if (rs.next()) {
                return parseAgtypeProperties(rs.getString(1));
            }
        }

        return null;
    }

    public Map<String, Object> getEdgeProperties(long edgeId) throws SQLException {
        String cypher = "MATCH ()-[e]->() WHERE id(e) = " + edgeId + " RETURN properties(e)";

        try (ResultSet rs = executeCypher(cypher)) {
            if (rs.next()) {
                return parseAgtypeProperties(rs.getString(1));
            }
        }

        return null;
    }

    public void syncVertexToShadow(long vertexId, Map<String, Object> properties) throws SQLException {
        String jsonb = toJsonbLiteral(properties);
        String sql = "INSERT INTO atlas_fti_vertex (vertex_id, properties) VALUES (" +
                vertexId + ", '" + jsonb + "'::jsonb) " +
                "ON CONFLICT (vertex_id) DO UPDATE SET properties = atlas_fti_vertex.properties || '" + jsonb + "'::jsonb";
        executeSqlUpdate(sql);
    }

    public void syncEdgeToShadow(long edgeId, Map<String, Object> properties) throws SQLException {
        String jsonb = toJsonbLiteral(properties);
        String sql = "INSERT INTO atlas_fti_edge (edge_id, properties) VALUES (" +
                edgeId + ", '" + jsonb + "'::jsonb) " +
                "ON CONFLICT (edge_id) DO UPDATE SET properties = atlas_fti_edge.properties || '" + jsonb + "'::jsonb";
        executeSqlUpdate(sql);
    }

    public void removeVertexFromShadow(long vertexId) throws SQLException {
        executeSqlUpdate("DELETE FROM atlas_fti_vertex WHERE vertex_id = " + vertexId);
    }

    public void removeEdgeFromShadow(long edgeId) throws SQLException {
        executeSqlUpdate("DELETE FROM atlas_fti_edge WHERE edge_id = " + edgeId);
    }

    private void ensureAgeLoaded(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("LOAD 'age'");
            stmt.execute("SET search_path = ag_catalog, \"$user\", public");
        }
    }

    static long extractAgtypeId(String agtypeValue) {
        if (agtypeValue == null) return -1;
        String trimmed = agtypeValue.trim();
        // AGE returns ids as plain integers or with ::bigint suffix
        if (trimmed.contains("::")) {
            trimmed = trimmed.substring(0, trimmed.indexOf("::"));
        }
        return Long.parseLong(trimmed.trim());
    }

    static Map<String, Object> parseAgtypeProperties(String agtype) {
        Map<String, Object> result = new HashMap<>();

        if (agtype == null || agtype.isEmpty()) {
            return result;
        }

        String trimmed = agtype.trim();

        // Strip agtype suffix if present
        if (trimmed.endsWith("::agtype")) {
            trimmed = trimmed.substring(0, trimmed.length() - "::agtype".length()).trim();
        }

        // Handle JSON object format from AGE
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1).trim();

            if (trimmed.isEmpty()) {
                return result;
            }

            // Simple key-value parser for AGE properties
            parseKeyValuePairs(trimmed, result);
        }

        return result;
    }

    private static void parseKeyValuePairs(String content, Map<String, Object> result) {
        int i = 0;
        int len = content.length();

        while (i < len) {
            // Skip whitespace
            while (i < len && Character.isWhitespace(content.charAt(i))) i++;
            if (i >= len) break;

            // Read key (quoted string)
            String key;
            if (content.charAt(i) == '"') {
                int end = content.indexOf('"', i + 1);
                key = content.substring(i + 1, end);
                i = end + 1;
            } else {
                int end = i;
                while (end < len && content.charAt(end) != ':') end++;
                key = content.substring(i, end).trim();
                i = end;
            }

            // Skip ': '
            while (i < len && (content.charAt(i) == ':' || content.charAt(i) == ' ')) i++;

            // Read value
            if (i >= len) break;

            Object value;
            char ch = content.charAt(i);

            if (ch == '"') {
                // String value — decode JSON/agtype escapes. The prior version dropped the
                // backslash and kept the next char literally, so `\n`→'n', `\t`→'t' etc. (the
                // round-trip dual of the shadow-sync escape bug): multi-line values like
                // AtlasGlossaryTerm.longDescription came back with newlines flattened to 'n'.
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < len && content.charAt(i) != '"') {
                    char c = content.charAt(i);
                    if (c == '\\' && i + 1 < len) {
                        char esc = content.charAt(i + 1);
                        i += 2;
                        switch (esc) {
                            case 'n': sb.append('\n'); break;
                            case 't': sb.append('\t'); break;
                            case 'r': sb.append('\r'); break;
                            case 'b': sb.append('\b'); break;
                            case 'f': sb.append('\f'); break;
                            case 'u':
                                if (i + 4 <= len) {
                                    try {
                                        sb.append((char) Integer.parseInt(content.substring(i, i + 4), 16));
                                        i += 4;
                                    } catch (NumberFormatException ex) {
                                        sb.append('u');
                                    }
                                } else {
                                    sb.append('u');
                                }
                                break;
                            default: sb.append(esc); // \" \\ \/ and any other → literal next char
                        }
                        continue;
                    }
                    sb.append(c);
                    i++;
                }
                i++; // skip closing quote
                value = sb.toString();
            } else if (ch == 't' || ch == 'f') {
                // Boolean
                if (content.startsWith("true", i)) {
                    value = Boolean.TRUE;
                    i += 4;
                } else {
                    value = Boolean.FALSE;
                    i += 5;
                }
            } else if (ch == 'n') {
                // null
                value = null;
                i += 4;
            } else {
                // Number
                int end = i;
                while (end < len && content.charAt(end) != ',' && content.charAt(end) != '}') end++;
                String numStr = content.substring(i, end).trim();
                if (numStr.contains(".")) {
                    value = Double.parseDouble(numStr);
                } else {
                    try {
                        value = Long.parseLong(numStr);
                    } catch (NumberFormatException e) {
                        value = numStr;
                    }
                }
                i = end;
            }

            result.put(key, value);

            // Skip comma
            while (i < len && (content.charAt(i) == ',' || content.charAt(i) == ' ')) i++;
        }
    }

    public static String toCypherValue(Object value) {
        if (value == null) return "NULL";
        if (value instanceof String) return "'" + escapeCypherString((String) value) + "'";
        if (value instanceof Boolean) return value.toString();
        if (value instanceof Number) return value.toString();
        if (value instanceof List) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object item : (List<?>) value) {
                if (!first) sb.append(", ");
                sb.append(toCypherValue(item));
                first = false;
            }
            sb.append("]");
            return sb.toString();
        }
        return "'" + escapeCypherString(value.toString()) + "'";
    }

    public static String escapeCypherString(String s) {
        return s.replace("\\", "\\\\").replace("'", "\\'");
    }

    public static String escapeCypherKey(String key) {
        // AGE property keys need backtick escaping if they contain dots or special chars
        if (key.contains(".") || key.contains("-") || key.contains(" ") || key.startsWith("__")) {
            return "`" + key + "`";
        }
        return key;
    }

    static String escapeCypherLabel(String label) {
        // Replace characters not valid in Cypher labels
        return label.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    static String toJsonbLiteral(Map<String, Object> properties) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(escapeJsonString(entry.getKey())).append("\":");

            Object val = entry.getValue();
            if (val == null) {
                sb.append("null");
            } else if (val instanceof String) {
                sb.append("\"").append(escapeJsonString((String) val)).append("\"");
            } else if (val instanceof Boolean || val instanceof Number) {
                sb.append(val);
            } else if (val instanceof List) {
                sb.append("[");
                boolean listFirst = true;
                for (Object item : (List<?>) val) {
                    if (!listFirst) sb.append(",");
                    if (item instanceof String) {
                        sb.append("\"").append(escapeJsonString((String) item)).append("\"");
                    } else {
                        sb.append(item);
                    }
                    listFirst = false;
                }
                sb.append("]");
            } else {
                sb.append("\"").append(escapeJsonString(val.toString())).append("\"");
            }

            first = false;
        }

        sb.append("}");
        return sb.toString();
    }

    /**
     * Escape a string for embedding as a JSON value inside a single-quoted SQL ::jsonb literal.
     * Two escaping layers at once: JSON-string escaping (backslash, double-quote, and the C0 control
     * characters, which Postgres' json/jsonb input rejects unescaped — "Character with value 0x0a
     * must be escaped") AND SQL single-quote doubling (the whole jsonb literal is wrapped in '...').
     * The prior version escaped only \ " ' and left raw newlines/tabs in multi-line property values
     * (e.g. AtlasGlossaryTerm.longDescription) — that aborted the shadow-sync transaction, poisoning
     * the whole term/entity update. [aegir/signals AGE-backend fork; upstreamable]
     */
    private static String escapeJsonString(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '"':  sb.append("\\\""); break;
                case '\'': sb.append("''");   break; // SQL literal: double the single quote
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                case '\b': sb.append("\\b");  break;
                case '\f': sb.append("\\f");  break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }
}
