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

import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.MatchingOperator;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.QueryOperator;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.SortOrder;
import org.apache.atlas.repository.graphdb.age.AgeCypherExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AgeQueryBuilder {
    private final List<Condition> conditions = new ArrayList<>();
    private String orderByKey;
    private SortOrder orderByDirection;

    public void has(String key, Object value) {
        conditions.add(new HasCondition(key, value));
    }

    public void in(String key, Collection<?> values) {
        conditions.add(new InCondition(key, values));
    }

    public void has(String key, QueryOperator op, Object value) {
        conditions.add(new OperatorCondition(key, op, value));
    }

    public void orderBy(String key, SortOrder order) {
        this.orderByKey = key;
        this.orderByDirection = order;
    }

    public void or(List<AgeQueryBuilder> childBuilders) {
        conditions.add(new OrCondition(childBuilders));
    }

    public void addConditionsFrom(AgeQueryBuilder other) {
        conditions.addAll(other.conditions);
        if (other.orderByKey != null) {
            this.orderByKey = other.orderByKey;
            this.orderByDirection = other.orderByDirection;
        }
    }

    public String buildWhereClause(String alias) {
        if (conditions.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;

        for (Condition cond : conditions) {
            if (!first) sb.append(" AND ");
            sb.append(cond.toCypher(alias));
            first = false;
        }

        return sb.toString();
    }

    public String buildOrderBy(String alias) {
        if (orderByKey == null) {
            return "";
        }
        String dir = orderByDirection == SortOrder.DESC ? "DESC" : "ASC";
        return " ORDER BY " + alias + "." + AgeCypherExecutor.escapeCypherKey(orderByKey) + " " + dir;
    }

    // Condition types

    private interface Condition {
        String toCypher(String alias);
    }

    private static class HasCondition implements Condition {
        final String key;
        final Object value;

        HasCondition(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toCypher(String alias) {
            return alias + "." + AgeCypherExecutor.escapeCypherKey(key) + " = " +
                    AgeCypherExecutor.toCypherValue(value);
        }
    }

    private static class InCondition implements Condition {
        final String key;
        final Collection<?> values;

        InCondition(String key, Collection<?> values) {
            this.key = key;
            this.values = values;
        }

        @Override
        public String toCypher(String alias) {
            StringBuilder sb = new StringBuilder();
            sb.append(alias).append(".").append(AgeCypherExecutor.escapeCypherKey(key)).append(" IN [");
            boolean first = true;
            for (Object val : values) {
                if (!first) sb.append(", ");
                sb.append(AgeCypherExecutor.toCypherValue(val));
                first = false;
            }
            sb.append("]");
            return sb.toString();
        }
    }

    private static class OperatorCondition implements Condition {
        final String key;
        final QueryOperator op;
        final Object value;

        OperatorCondition(String key, QueryOperator op, Object value) {
            this.key = key;
            this.op = op;
            this.value = value;
        }

        @Override
        public String toCypher(String alias) {
            String escapedKey = alias + "." + AgeCypherExecutor.escapeCypherKey(key);

            if (op instanceof ComparisionOperator) {
                String cypherOp;
                switch ((ComparisionOperator) op) {
                    case EQUAL:              cypherOp = "="; break;
                    case NOT_EQUAL:          cypherOp = "<>"; break;
                    case GREATER_THAN:       cypherOp = ">"; break;
                    case GREATER_THAN_EQUAL: cypherOp = ">="; break;
                    case LESS_THAN:          cypherOp = "<"; break;
                    case LESS_THAN_EQUAL:    cypherOp = "<="; break;
                    default:                 cypherOp = "="; break;
                }
                return escapedKey + " " + cypherOp + " " + AgeCypherExecutor.toCypherValue(value);
            }

            if (op instanceof MatchingOperator) {
                String val = value != null ? value.toString() : "";
                switch ((MatchingOperator) op) {
                    case CONTAINS:
                        return escapedKey + " =~ '.*" + escapeRegex(val) + ".*'";
                    case PREFIX:
                        return escapedKey + " =~ '^" + escapeRegex(val) + ".*'";
                    case SUFFIX:
                        return escapedKey + " =~ '.*" + escapeRegex(val) + "$'";
                    case REGEX:
                        return escapedKey + " =~ '" + AgeCypherExecutor.escapeCypherString(val) + "'";
                    default:
                        return escapedKey + " = " + AgeCypherExecutor.toCypherValue(value);
                }
            }

            return escapedKey + " = " + AgeCypherExecutor.toCypherValue(value);
        }

        private String escapeRegex(String s) {
            return AgeCypherExecutor.escapeCypherString(
                    s.replaceAll("([.+*?\\[\\](){}|^$\\\\])", "\\\\$1"));
        }
    }

    private static class OrCondition implements Condition {
        final List<AgeQueryBuilder> children;

        OrCondition(List<AgeQueryBuilder> children) {
            this.children = children;
        }

        @Override
        public String toCypher(String alias) {
            StringBuilder sb = new StringBuilder("(");
            boolean first = true;

            for (AgeQueryBuilder child : children) {
                String childWhere = child.buildWhereClause(alias);
                if (!childWhere.isEmpty()) {
                    if (!first) sb.append(" OR ");
                    sb.append("(").append(childWhere).append(")");
                    first = false;
                }
            }

            sb.append(")");
            return sb.toString();
        }
    }
}
