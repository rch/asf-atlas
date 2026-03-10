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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Solr/Atlas-style index query strings into PostgreSQL tsquery and field filters.
 *
 * Input format examples:
 *   v."__typeName":hive_table AND v."__state":ACTIVE
 *   v."__fullText":some_search_term
 *   *:*
 */
public class SolrToTsqueryParser {
    private static final Pattern FIELD_PATTERN = Pattern.compile("v\\.\"([^\"]+)\":([^\\s]+)");
    private static final Pattern WILDCARD_PATTERN = Pattern.compile("\\*:\\*");

    public static ParsedQuery parse(String queryString) {
        if (queryString == null || queryString.trim().isEmpty() || WILDCARD_PATTERN.matcher(queryString.trim()).matches()) {
            return new ParsedQuery(null, new ArrayList<>());
        }

        // Strip the element identifier prefix (e.g., "v." in "v.\"__typeName\":value")
        String normalized = queryString.trim();
        // Remove leading identifier like "v." or "e."
        if (normalized.startsWith("v.") || normalized.startsWith("e.")) {
            // Already handled by regex
        }

        List<FieldFilter> filters = new ArrayList<>();
        List<String> freeTextTerms = new ArrayList<>();

        // Split on AND
        String[] parts = normalized.split("\\s+AND\\s+");

        for (String part : parts) {
            part = part.trim();
            Matcher matcher = FIELD_PATTERN.matcher(part);

            if (matcher.matches()) {
                String field = matcher.group(1);
                String value = matcher.group(2);

                // __fullText is a special field — route to tsvector search
                if ("__fullText".equals(field)) {
                    freeTextTerms.add(sanitizeTsqueryTerm(value));
                } else {
                    filters.add(new FieldFilter(field, unquote(value)));
                }
            } else {
                // Treat as free-text search term
                String term = part.replaceAll("[\"()]", "").trim();
                if (!term.isEmpty() && !"*:*".equals(term)) {
                    freeTextTerms.add(sanitizeTsqueryTerm(term));
                }
            }
        }

        String tsquery = freeTextTerms.isEmpty() ? null : String.join(" & ", freeTextTerms);

        return new ParsedQuery(tsquery, filters);
    }

    private static String sanitizeTsqueryTerm(String term) {
        // Remove special characters that would break tsquery
        String cleaned = term.replaceAll("[^a-zA-Z0-9_.*]", "");

        if (cleaned.isEmpty()) {
            return "";
        }

        // Handle wildcards: convert * to :* for prefix matching
        if (cleaned.endsWith("*")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1) + ":*";
        }

        return cleaned;
    }

    private static String unquote(String value) {
        if (value != null && value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    public static class ParsedQuery {
        private final String tsquery;
        private final List<FieldFilter> fieldFilters;

        ParsedQuery(String tsquery, List<FieldFilter> fieldFilters) {
            this.tsquery = tsquery;
            this.fieldFilters = fieldFilters;
        }

        public boolean hasTsquery() {
            return tsquery != null && !tsquery.isEmpty();
        }

        public String getTsquery() {
            return tsquery;
        }

        public List<FieldFilter> getFieldFilters() {
            return fieldFilters;
        }
    }

    public static class FieldFilter {
        private final String field;
        private final String value;

        FieldFilter(String field, String value) {
            this.field = field;
            this.value = value;
        }

        public String getField() {
            return field;
        }

        public String getValue() {
            return value;
        }
    }
}
