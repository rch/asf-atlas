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
package org.apache.atlas.repository.graphdb.age.query;

import org.apache.atlas.repository.graphdb.age.AgeCypherExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Limited Gremlin-to-Cypher translator covering the ~30 operations used in
 * EntityDiscoveryService.searchUsingBasicQuery():
 *   g.V().has(key, value).has(key, op, value).hasNot(key).range(start, end)
 *        .order().by(key, order).toList().count()
 */
public class GremlinToCypherTranslator {
    private static final Logger LOG = LoggerFactory.getLogger(GremlinToCypherTranslator.class);

    private static final Pattern HAS_KV      = Pattern.compile("\\.has\\('([^']+)',\\s*'([^']*)'\\)");
    private static final Pattern HAS_NOT     = Pattern.compile("\\.hasNot\\('([^']+)'\\)");
    private static final Pattern RANGE       = Pattern.compile("\\.range\\((\\d+),\\s*(\\d+)\\)");
    private static final Pattern ORDER_BY    = Pattern.compile("\\.order\\(\\)\\.by\\('([^']+)',\\s*(asc|desc|incr|decr)\\)");
    private static final Pattern COUNT       = Pattern.compile("\\.count\\(\\)");

    public static String translate(String gremlin) {
        if (gremlin == null || gremlin.trim().isEmpty()) {
            return "MATCH (n:vertex) RETURN n";
        }

        String g = gremlin.trim();

        // Must start with g.V()
        if (!g.startsWith("g.V()")) {
            throw new UnsupportedOperationException("Only g.V() traversals are supported. Got: " + g);
        }

        List<String> whereClauses = new ArrayList<>();
        String orderBy = "";
        String pagination = "";
        boolean isCount = false;

        // Extract has(key, value) clauses
        Matcher hasKv = HAS_KV.matcher(g);
        while (hasKv.find()) {
            String key = hasKv.group(1);
            String value = hasKv.group(2);
            whereClauses.add("n." + AgeCypherExecutor.escapeCypherKey(key) + " = '" +
                    AgeCypherExecutor.escapeCypherString(value) + "'");
        }

        // Extract hasNot(key) clauses
        Matcher hasNot = HAS_NOT.matcher(g);
        while (hasNot.find()) {
            String key = hasNot.group(1);
            whereClauses.add("n." + AgeCypherExecutor.escapeCypherKey(key) + " IS NULL");
        }

        // Extract range(start, end)
        Matcher range = RANGE.matcher(g);
        if (range.find()) {
            int start = Integer.parseInt(range.group(1));
            int end = Integer.parseInt(range.group(2));
            pagination = " SKIP " + start + " LIMIT " + (end - start);
        }

        // Extract order().by(key, order)
        Matcher order = ORDER_BY.matcher(g);
        if (order.find()) {
            String key = order.group(1);
            String dir = order.group(2);
            String cypherDir = ("desc".equalsIgnoreCase(dir) || "decr".equalsIgnoreCase(dir)) ? "DESC" : "ASC";
            orderBy = " ORDER BY n." + AgeCypherExecutor.escapeCypherKey(key) + " " + cypherDir;
        }

        // Check for count()
        if (COUNT.matcher(g).find()) {
            isCount = true;
        }

        // Build Cypher
        StringBuilder cypher = new StringBuilder("MATCH (n:vertex)");

        if (!whereClauses.isEmpty()) {
            cypher.append(" WHERE ").append(String.join(" AND ", whereClauses));
        }

        if (isCount) {
            cypher.append(" RETURN count(n)");
        } else {
            cypher.append(orderBy);
            cypher.append(pagination);
            cypher.append(" RETURN n");
        }

        LOG.debug("Translated Gremlin '{}' to Cypher '{}'", gremlin, cypher);

        return cypher.toString();
    }
}
