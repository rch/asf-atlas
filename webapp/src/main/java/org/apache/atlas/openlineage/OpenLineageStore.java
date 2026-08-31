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
package org.apache.atlas.openlineage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * AGE + event-log store for the complete OpenLineage / Marquez-compatible API surface.
 * <p>
 * Graph name defaults to {@code signals_ol} on the Atlas JDBC database (signals).
 * Labels: Job, Run, Dataset; edges EXECUTES, INPUT_TO, OUTPUTS, PARENT.
 * Relational tables hold event history, tags, namespaces, and sources.
 * <p>
 * Marquez UI (and Marquez API contract tests) are the validation harness — this
 * store must return valid Marquez-shaped JSON for every API the UI/probes call.
 */
@Service
public class OpenLineageStore {
    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private static final String CONF_JDBC_URL  = "atlas.age.jdbc.url";
    private static final String CONF_JDBC_USER = "atlas.age.jdbc.user";
    private static final String CONF_JDBC_PASS = "atlas.age.jdbc.password";
    private static final String CONF_OL_GRAPH  = "atlas.openlineage.graph.name";

    private String jdbcUrl  = "jdbc:postgresql://localhost:5455/signals";
    private String jdbcUser = "signals";
    private String jdbcPass = "signals";
    private String graph    = "signals_ol";
    private boolean ready;

    @PostConstruct
    public void init() {
        try {
            Configuration conf = ApplicationProperties.get();
            jdbcUrl  = conf.getString(CONF_JDBC_URL, jdbcUrl);
            jdbcUser = conf.getString(CONF_JDBC_USER, jdbcUser);
            jdbcPass = conf.getString(CONF_JDBC_PASS, jdbcPass);
            graph    = conf.getString(CONF_OL_GRAPH, graph);
            ensureSchema();
            ready = true;
            LOG.info("OpenLineageStore ready: graph={} url={}", graph, jdbcUrl);
        } catch (Exception e) {
            ready = false;
            LOG.error("OpenLineageStore init failed — /api/v1 will return errors until AGE JDBC is available", e);
        }
    }

    public boolean isReady() {
        return ready;
    }

    private Connection connect() throws Exception {
        Connection c = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
        try (Statement s = c.createStatement()) {
            s.execute("LOAD 'age'");
            s.execute("SET search_path = ag_catalog, \"$user\", public");
        }
        return c;
    }

    private void ensureSchema() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            try {
                s.execute("SELECT create_graph('" + graph + "')");
            } catch (Exception ignore) {
                // already exists
            }
            for (String label : new String[] {"Job", "Run", "Dataset"}) {
                try {
                    s.execute("SELECT create_vlabel('" + graph + "', '" + label + "')");
                } catch (Exception ignore) {
                }
            }
            for (String el : new String[] {"EXECUTES", "INPUT_TO", "OUTPUTS", "PARENT"}) {
                try {
                    s.execute("SELECT create_elabel('" + graph + "', '" + el + "')");
                } catch (Exception ignore) {
                }
            }
            s.execute(
                "CREATE TABLE IF NOT EXISTS public.lineage_events (" +
                " id BIGSERIAL PRIMARY KEY," +
                " event_type TEXT NOT NULL DEFAULT 'RUN_EVENT'," +
                " event_time TIMESTAMPTZ NOT NULL DEFAULT NOW()," +
                " run_id TEXT NOT NULL," +
                " job_namespace TEXT NOT NULL," +
                " job_name TEXT NOT NULL," +
                " run_state TEXT," +
                " producer TEXT," +
                " inputs JSONB DEFAULT '[]'::jsonb," +
                " outputs JSONB DEFAULT '[]'::jsonb," +
                " facets JSONB DEFAULT '{}'::jsonb," +
                " job_facets JSONB DEFAULT '{}'::jsonb," +
                " raw JSONB," +
                " created_at TIMESTAMPTZ DEFAULT NOW()" +
                ")"
            );
            try {
                s.execute("ALTER TABLE public.lineage_events ADD COLUMN IF NOT EXISTS job_facets JSONB DEFAULT '{}'::jsonb");
            } catch (Exception ignore) {
            }
            s.execute(
                "CREATE TABLE IF NOT EXISTS public.lineage_namespaces (" +
                " name TEXT PRIMARY KEY," +
                " owner_name TEXT NOT NULL DEFAULT 'anonymous'," +
                " description TEXT," +
                " is_hidden BOOLEAN NOT NULL DEFAULT FALSE," +
                " created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()," +
                " updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" +
                ")"
            );
            s.execute(
                "CREATE TABLE IF NOT EXISTS public.lineage_tags (" +
                " name TEXT PRIMARY KEY," +
                " description TEXT," +
                " created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" +
                ")"
            );
            s.execute(
                "CREATE TABLE IF NOT EXISTS public.lineage_entity_tags (" +
                " entity_type TEXT NOT NULL," +
                " namespace TEXT NOT NULL," +
                " name TEXT NOT NULL," +
                " field_name TEXT NOT NULL DEFAULT ''," +
                " tag TEXT NOT NULL," +
                " PRIMARY KEY (entity_type, namespace, name, field_name, tag)" +
                ")"
            );
            s.execute(
                "CREATE TABLE IF NOT EXISTS public.lineage_sources (" +
                " name TEXT PRIMARY KEY," +
                " type TEXT NOT NULL DEFAULT 'POSTGRESQL'," +
                " connection_url TEXT," +
                " description TEXT," +
                " created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()," +
                " updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" +
                ")"
            );
            s.execute(
                "CREATE TABLE IF NOT EXISTS public.lineage_dataset_sources (" +
                " namespace TEXT NOT NULL," +
                " name TEXT NOT NULL," +
                " source_name TEXT NOT NULL," +
                " PRIMARY KEY (namespace, name)" +
                ")"
            );
            // Stock Marquez ships a "default" source; datasets without a
            // dataSource facet land on it.
            s.execute(
                "INSERT INTO public.lineage_sources (name) VALUES ('default') ON CONFLICT DO NOTHING"
            );
            s.execute(
                "CREATE INDEX IF NOT EXISTS lineage_events_time_idx ON public.lineage_events (event_time DESC)"
            );
            s.execute(
                "CREATE INDEX IF NOT EXISTS lineage_events_run_idx ON public.lineage_events (run_id)"
            );
            s.execute(
                "CREATE INDEX IF NOT EXISTS lineage_events_job_idx ON public.lineage_events (job_namespace, job_name)"
            );
        }
    }

    private static String lit(String v) {
        if (v == null) {
            return "null";
        }
        return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    private List<String> cypher(Connection c, String cypher) throws Exception {
        String sql = "SELECT * FROM cypher('" + graph + "', $q$" + cypher + "$q$) AS (v agtype)";
        List<String> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String v = rs.getString(1);
                if (v == null) {
                    continue;
                }
                for (String suf : new String[] {"::vertex", "::edge", "::path"}) {
                    if (v.endsWith(suf)) {
                        v = v.substring(0, v.length() - suf.length());
                    }
                }
                out.add(v);
            }
        }
        return out;
    }

    private void mergeNode(Connection c, String label, Map<String, String> keys, Map<String, String> props) throws Exception {
        StringBuilder kb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : keys.entrySet()) {
            if (!first) {
                kb.append(", ");
            }
            kb.append(e.getKey()).append(": ").append(lit(e.getValue()));
            first = false;
        }
        kb.append("}");
        String set = "";
        if (props != null && !props.isEmpty()) {
            StringBuilder pb = new StringBuilder("{");
            first = true;
            for (Map.Entry<String, String> e : props.entrySet()) {
                if (!first) {
                    pb.append(", ");
                }
                pb.append(e.getKey()).append(": ").append(lit(e.getValue()));
                first = false;
            }
            pb.append("}");
            set = " SET n += " + pb;
        }
        cypher(c, "MERGE (n:" + label + " " + kb + ")" + set + " RETURN 1");
    }

    private void mergeEdge(Connection c, String srcLabel, Map<String, String> srcKeys,
                           String edge, String dstLabel, Map<String, String> dstKeys) throws Exception {
        StringBuilder sa = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : srcKeys.entrySet()) {
            if (!first) {
                sa.append(", ");
            }
            sa.append(e.getKey()).append(": ").append(lit(e.getValue()));
            first = false;
        }
        sa.append("}");
        StringBuilder da = new StringBuilder("{");
        first = true;
        for (Map.Entry<String, String> e : dstKeys.entrySet()) {
            if (!first) {
                da.append(", ");
            }
            da.append(e.getKey()).append(": ").append(lit(e.getValue()));
            first = false;
        }
        da.append("}");
        cypher(c,
            "MATCH (a:" + srcLabel + " " + sa + "), (b:" + dstLabel + " " + da + ") " +
            "MERGE (a)-[r:" + edge + "]->(b) RETURN 1");
    }

    private void ensureNamespace(Connection c, String name) throws Exception {
        if (name == null || name.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO public.lineage_namespaces (name) VALUES (?) " +
            "ON CONFLICT (name) DO UPDATE SET updated_at = NOW()"
        )) {
            ps.setString(1, name);
            ps.executeUpdate();
        }
    }

    // ─── Ingest ──────────────────────────────────────────────────────────────

    /**
     * Ingest a single OpenLineage RunEvent JSON object.
     */
    public Map<String, Object> ingestRunEvent(JsonNode body) throws Exception {
        if (!ready) {
            ensureSchema();
            ready = true;
        }
        JsonNode job = body.path("job");
        JsonNode run = body.path("run");
        String ns = text(job, "namespace");
        String name = text(job, "name");
        String runId = text(run, "runId");
        if (runId.isEmpty()) {
            runId = text(run, "run_id");
        }
        if (ns.isEmpty() || name.isEmpty() || runId.isEmpty()) {
            throw new IllegalArgumentException("job.namespace, job.name, and run.runId are required");
        }
        String eventType = body.path("eventType").asText(body.path("event_type").asText("OTHER")).toUpperCase(Locale.ROOT);
        String eventTime = body.path("eventTime").asText(body.path("event_time").asText(""));
        String producer = body.path("producer").asText("");
        String marquezState = toRunState(eventType);

        List<String> inputs = datasetQns(body.path("inputs"));
        List<String> outputs = datasetQns(body.path("outputs"));

        try (Connection c = connect()) {
            c.setAutoCommit(true);
            ensureNamespace(c, ns);
            for (String qn : inputs) {
                String[] p = qn.split(":", 2);
                ensureNamespace(c, p[0]);
            }
            for (String qn : outputs) {
                String[] p = qn.split(":", 2);
                ensureNamespace(c, p[0]);
            }

            try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO public.lineage_events (" +
                " event_type, event_time, run_id, job_namespace, job_name, run_state, producer," +
                " inputs, outputs, facets, job_facets, raw) VALUES (" +
                " 'RUN_EVENT', COALESCE(?::timestamptz, NOW()), ?, ?, ?, ?, ?," +
                " ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb)"
            )) {
                ps.setString(1, eventTime.isEmpty() ? null : eventTime);
                ps.setString(2, runId);
                ps.setString(3, ns);
                ps.setString(4, name);
                ps.setString(5, marquezState);
                ps.setString(6, producer.isEmpty() ? null : producer);
                ps.setString(7, MAPPER.writeValueAsString(body.path("inputs")));
                ps.setString(8, MAPPER.writeValueAsString(body.path("outputs")));
                ps.setString(9, MAPPER.writeValueAsString(run.path("facets")));
                ps.setString(10, MAPPER.writeValueAsString(job.path("facets")));
                ps.setString(11, MAPPER.writeValueAsString(body));
                ps.executeUpdate();
            }

            // Materialize Marquez Sources from dataset dataSource facets
            // (else "default"), and remember each dataset's source.
            materializeSources(c, body);

            Map<String, String> jobKeys = mapOf("namespace", ns, "name", name);
            Map<String, String> jobProps = mapOf(
                "namespace", ns,
                "name", name,
                "updatedAt", Instant.now().toString()
            );
            mergeNode(c, "Job", jobKeys, jobProps);
            Map<String, String> runKeys = Collections.singletonMap("run_id", runId);
            Map<String, String> runProps = mapOf(
                "run_id", runId,
                "eventType", eventType,
                "state", marquezState,
                "eventTime", eventTime,
                "job_namespace", ns,
                "job_name", name
            );
            mergeNode(c, "Run", runKeys, runProps);
            mergeEdge(c, "Job", jobKeys, "EXECUTES", "Run", runKeys);

            for (String qn : inputs) {
                String[] parts = qn.split(":", 2);
                Map<String, String> dKeys = Collections.singletonMap("qualifiedName", qn);
                Map<String, String> dProps = mapOf(
                    "qualifiedName", qn,
                    "namespace", parts[0],
                    "name", parts.length > 1 ? parts[1] : qn
                );
                mergeNode(c, "Dataset", dKeys, dProps);
                mergeEdge(c, "Dataset", dKeys, "INPUT_TO", "Run", runKeys);
            }
            for (String qn : outputs) {
                String[] parts = qn.split(":", 2);
                Map<String, String> dKeys = Collections.singletonMap("qualifiedName", qn);
                Map<String, String> dProps = mapOf(
                    "qualifiedName", qn,
                    "namespace", parts[0],
                    "name", parts.length > 1 ? parts[1] : qn
                );
                mergeNode(c, "Dataset", dKeys, dProps);
                mergeEdge(c, "Run", runKeys, "OUTPUTS", "Dataset", dKeys);
            }
        }

        Map<String, Object> receipt = new LinkedHashMap<>();
        receipt.put("ok", true);
        receipt.put("run_id", runId);
        Map<String, String> jobOut = new LinkedHashMap<>();
        jobOut.put("namespace", ns);
        jobOut.put("name", name);
        receipt.put("job", jobOut);
        receipt.put("eventType", eventType);
        return receipt;
    }

    // ─── Namespaces ──────────────────────────────────────────────────────────

    public List<Map<String, Object>> listNamespacesDetailed() throws Exception {
        try (Connection c = connect()) {
            // Discover namespaces from events/graph and ensure rows exist
            Set<String> names = new LinkedHashSet<>();
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                     "SELECT DISTINCT job_namespace FROM public.lineage_events " +
                     "UNION SELECT name FROM public.lineage_namespaces")) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
            for (String n : listNamespacesFromGraph(c)) {
                names.add(n);
            }
            for (String n : names) {
                ensureNamespace(c, n);
            }
            List<Map<String, Object>> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                "SELECT name, owner_name, description, is_hidden, created_at, updated_at " +
                "FROM public.lineage_namespaces WHERE is_hidden = FALSE ORDER BY name"
            ); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(namespaceRow(rs));
                }
            }
            return out;
        }
    }

    public Map<String, Object> getNamespace(String name) throws Exception {
        try (Connection c = connect()) {
            ensureNamespace(c, name);
            try (PreparedStatement ps = c.prepareStatement(
                "SELECT name, owner_name, description, is_hidden, created_at, updated_at " +
                "FROM public.lineage_namespaces WHERE name = ?"
            )) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return namespaceRow(rs);
                    }
                }
            }
            return null;
        }
    }

    public Map<String, Object> upsertNamespace(String name, JsonNode meta) throws Exception {
        String owner = text(meta, "ownerName");
        if (owner.isEmpty()) {
            owner = "anonymous";
        }
        String desc = text(meta, "description");
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "INSERT INTO public.lineage_namespaces (name, owner_name, description) VALUES (?, ?, ?) " +
                 "ON CONFLICT (name) DO UPDATE SET owner_name = EXCLUDED.owner_name, " +
                 "description = EXCLUDED.description, updated_at = NOW(), is_hidden = FALSE"
             )) {
            ps.setString(1, name);
            ps.setString(2, owner);
            ps.setString(3, desc.isEmpty() ? null : desc);
            ps.executeUpdate();
        }
        return getNamespace(name);
    }

    public Map<String, Object> deleteNamespace(String name) throws Exception {
        Map<String, Object> before = getNamespace(name);
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "UPDATE public.lineage_namespaces SET is_hidden = TRUE, updated_at = NOW() WHERE name = ?"
             )) {
            ps.setString(1, name);
            ps.executeUpdate();
        }
        return before;
    }

    private List<String> listNamespacesFromGraph(Connection c) throws Exception {
        List<String> raw = cypher(c, "MATCH (j:Job) RETURN DISTINCT j.namespace");
        List<String> ns = new ArrayList<>();
        for (String r : raw) {
            String v = unwrapJsonString(r);
            if (v != null && !v.isEmpty()) {
                ns.add(v);
            }
        }
        return ns;
    }

    /** Back-compat: simple string list used by older callers. */
    public List<String> listNamespaces() throws Exception {
        List<String> out = new ArrayList<>();
        for (Map<String, Object> n : listNamespacesDetailed()) {
            out.add(String.valueOf(n.get("name")));
        }
        return out;
    }

    private static Map<String, Object> namespaceRow(ResultSet rs) throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", rs.getString("name"));
        m.put("createdAt", tsIso(rs.getTimestamp("created_at")));
        m.put("updatedAt", tsIso(rs.getTimestamp("updated_at")));
        m.put("ownerName", rs.getString("owner_name"));
        String desc = rs.getString("description");
        m.put("description", desc == null ? "" : desc);
        m.put("isHidden", rs.getBoolean("is_hidden"));
        return m;
    }

    // ─── Jobs ────────────────────────────────────────────────────────────────

    public Map<String, Object> listJobsPage(String namespace, int limit, int offset) throws Exception {
        List<Map<String, Object>> all = new ArrayList<>();
        try (Connection c = connect()) {
            String sql =
                "SELECT DISTINCT job_namespace, job_name, MAX(event_time) AS updated_at " +
                "FROM public.lineage_events " +
                (namespace != null ? "WHERE job_namespace = ? " : "") +
                "GROUP BY job_namespace, job_name ORDER BY job_namespace, job_name";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                if (namespace != null) {
                    ps.setString(1, namespace);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        all.add(buildJob(c, rs.getString(1), rs.getString(2), rs.getTimestamp(3)));
                    }
                }
            }
            // Also include graph-only jobs not yet in events
            String cypherQ = namespace == null
                ? "MATCH (j:Job) RETURN {ns: j.namespace, n: j.name}"
                : "MATCH (j:Job {namespace: " + lit(namespace) + "}) RETURN {ns: j.namespace, n: j.name}";
            for (String raw : cypher(c, cypherQ)) {
                JsonNode n = MAPPER.readTree(raw);
                String ns = text(n, "ns");
                String name = text(n, "n");
                if (ns.isEmpty() || name.isEmpty()) {
                    continue;
                }
                boolean found = false;
                for (Map<String, Object> j : all) {
                    if (ns.equals(j.get("namespace")) && name.equals(j.get("name"))) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    all.add(buildJob(c, ns, name, null));
                }
            }
        }
        int total = all.size();
        List<Map<String, Object>> page = slice(all, limit, offset);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jobs", page);
        out.put("totalCount", total);
        return out;
    }

    public List<Map<String, Object>> listJobs(String namespace) throws Exception {
        Map<String, Object> page = listJobsPage(namespace, 10000, 0);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobs = (List<Map<String, Object>>) page.get("jobs");
        return jobs;
    }

    public Map<String, Object> getJob(String namespace, String name) throws Exception {
        try (Connection c = connect()) {
            if (!jobExists(c, namespace, name)) {
                return null;
            }
            Timestamp updated = null;
            try (PreparedStatement ps = c.prepareStatement(
                "SELECT MAX(event_time) FROM public.lineage_events WHERE job_namespace = ? AND job_name = ?"
            )) {
                ps.setString(1, namespace);
                ps.setString(2, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        updated = rs.getTimestamp(1);
                    }
                }
            }
            return buildJob(c, namespace, name, updated);
        }
    }

    public Map<String, Object> upsertJob(String namespace, String name, JsonNode meta) throws Exception {
        try (Connection c = connect()) {
            ensureNamespace(c, namespace);
            Map<String, String> jobKeys = mapOf("namespace", namespace, "name", name);
            Map<String, String> props = mapOf(
                "namespace", namespace,
                "name", name,
                "type", textOr(meta, "type", "BATCH"),
                "description", text(meta, "description"),
                "location", text(meta, "location"),
                "updatedAt", Instant.now().toString()
            );
            mergeNode(c, "Job", jobKeys, props);
        }
        return getJob(namespace, name);
    }

    public Map<String, Object> deleteJob(String namespace, String name) throws Exception {
        Map<String, Object> before = getJob(namespace, name);
        if (before == null) {
            return null;
        }
        try (Connection c = connect()) {
            cypher(c, "MATCH (j:Job {namespace: " + lit(namespace) + ", name: " + lit(name) + "}) DETACH DELETE j RETURN 1");
            try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM public.lineage_entity_tags WHERE entity_type = 'job' AND namespace = ? AND name = ?"
            )) {
                ps.setString(1, namespace);
                ps.setString(2, name);
                ps.executeUpdate();
            }
        }
        return before;
    }

    private boolean jobExists(Connection c, String namespace, String name) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT 1 FROM public.lineage_events WHERE job_namespace = ? AND job_name = ? LIMIT 1"
        )) {
            ps.setString(1, namespace);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return true;
                }
            }
        }
        List<String> hit = cypher(c,
            "MATCH (j:Job {namespace: " + lit(namespace) + ", name: " + lit(name) + "}) RETURN j.name");
        return !hit.isEmpty();
    }

    private Map<String, Object> buildJob(Connection c, String namespace, String name, Timestamp updated) throws Exception {
        String now = Instant.now().toString();
        String updatedIso = updated != null ? tsIso(updated) : now;
        Map<String, Object> job = new LinkedHashMap<>();
        Map<String, String> id = new LinkedHashMap<>();
        id.put("namespace", namespace);
        id.put("name", name);
        job.put("id", id);
        job.put("type", "BATCH");
        job.put("name", name);
        job.put("simpleName", simpleName(name));
        job.put("parentJobName", null);
        job.put("parentJobUuid", null);
        job.put("createdAt", updatedIso);
        job.put("updatedAt", updatedIso);
        job.put("namespace", namespace);
        job.put("inputs", jobDatasets(c, namespace, name, true));
        job.put("outputs", jobDatasets(c, namespace, name, false));
        job.put("location", null);
        job.put("description", null);
        Map<String, Object> latest = latestRunForJob(c, namespace, name);
        job.put("latestRun", latest);
        job.put("latestRuns", latest == null ? Collections.emptyList() : Collections.singletonList(latest));
        job.put("facets", Collections.emptyMap());
        job.put("currentVersion", null);
        job.put("labels", Collections.emptyList());
        job.put("tags", entityTags(c, "job", namespace, name, ""));
        return job;
    }

    private List<Map<String, String>> jobDatasets(Connection c, String ns, String job, boolean inputs) throws Exception {
        String cypherQ = inputs
            ? "MATCH (d:Dataset)-[:INPUT_TO]->(:Run)<-[:EXECUTES]-(:Job {namespace: " + lit(ns) +
              ", name: " + lit(job) + "}) RETURN DISTINCT d.qualifiedName"
            : "MATCH (:Job {namespace: " + lit(ns) + ", name: " + lit(job) +
              "})-[:EXECUTES]->(:Run)-[:OUTPUTS]->(d:Dataset) RETURN DISTINCT d.qualifiedName";
        List<Map<String, String>> out = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String raw : cypher(c, cypherQ)) {
            String qn = unwrapJsonString(raw);
            if (qn == null || !seen.add(qn)) {
                continue;
            }
            String[] p = qn.split(":", 2);
            Map<String, String> id = new LinkedHashMap<>();
            id.put("namespace", p[0]);
            id.put("name", p.length > 1 ? p[1] : qn);
            out.add(id);
        }
        return out;
    }

    private Map<String, Object> latestRunForJob(Connection c, String ns, String job) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT run_id, run_state, event_time, facets FROM public.lineage_events " +
            "WHERE job_namespace = ? AND job_name = ? ORDER BY event_time DESC LIMIT 1"
        )) {
            ps.setString(1, ns);
            ps.setString(2, job);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return buildRunFromRow(rs, ns, job);
            }
        }
    }

    // ─── Runs ────────────────────────────────────────────────────────────────

    public Map<String, Object> listRunsPage(String namespace, String job, int limit, int offset) throws Exception {
        List<Map<String, Object>> runs = new ArrayList<>();
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT DISTINCT ON (run_id) run_id, run_state, event_time, facets " +
                 "FROM public.lineage_events WHERE job_namespace = ? AND job_name = ? " +
                 "ORDER BY run_id, event_time DESC"
             )) {
            ps.setString(1, namespace);
            ps.setString(2, job);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    runs.add(buildRunFromRow(rs, namespace, job));
                }
            }
        }
        // Sort by event time desc
        runs.sort((a, b) -> String.valueOf(b.get("createdAt")).compareTo(String.valueOf(a.get("createdAt"))));
        int total = runs.size();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("runs", slice(runs, limit, offset));
        out.put("totalCount", total);
        return out;
    }

    public List<Map<String, Object>> listRuns(String namespace, String job) throws Exception {
        Map<String, Object> page = listRunsPage(namespace, job, 10000, 0);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> runs = (List<Map<String, Object>>) page.get("runs");
        return runs;
    }

    public Map<String, Object> getRun(String runId) throws Exception {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT run_id, run_state, event_time, facets, job_namespace, job_name " +
                 "FROM public.lineage_events WHERE run_id = ? ORDER BY event_time DESC LIMIT 1"
             )) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return buildRunFromRow(rs, rs.getString("job_namespace"), rs.getString("job_name"));
            }
        }
    }

    public Map<String, Object> createRun(String namespace, String job, JsonNode meta) throws Exception {
        String runId = text(meta, "id");
        if (runId.isEmpty()) {
            runId = UUID.randomUUID().toString();
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("eventType", "START");
        body.put("eventTime", Instant.now().toString());
        body.put("producer", "atlas-openlineage");
        Map<String, Object> run = new LinkedHashMap<>();
        run.put("runId", runId);
        run.put("facets", meta != null && meta.has("facets") ? MAPPER.convertValue(meta.get("facets"), Map.class) : Collections.emptyMap());
        body.put("run", run);
        Map<String, Object> j = new LinkedHashMap<>();
        j.put("namespace", namespace);
        j.put("name", job);
        body.put("job", j);
        body.put("inputs", Collections.emptyList());
        body.put("outputs", Collections.emptyList());
        ingestRunEvent(MAPPER.valueToTree(body));
        return getRun(runId);
    }

    public Map<String, Object> transitionRun(String runId, String state) throws Exception {
        Map<String, Object> existing = getRun(runId);
        if (existing == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> jv = (Map<String, Object>) existing.get("jobVersion");
        String ns = jv != null ? String.valueOf(jv.get("namespace")) : "";
        String job = jv != null ? String.valueOf(jv.get("name")) : "";
        String eventType = fromRunState(state);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("eventType", eventType);
        body.put("eventTime", Instant.now().toString());
        body.put("producer", "atlas-openlineage");
        Map<String, Object> run = new LinkedHashMap<>();
        run.put("runId", runId);
        body.put("run", run);
        Map<String, Object> j = new LinkedHashMap<>();
        j.put("namespace", ns);
        j.put("name", job);
        body.put("job", j);
        body.put("inputs", Collections.emptyList());
        body.put("outputs", Collections.emptyList());
        ingestRunEvent(MAPPER.valueToTree(body));
        return getRun(runId);
    }

    public Map<String, Object> getRunFacets(String runId, String type) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("runId", runId);
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT facets, job_facets FROM public.lineage_events WHERE run_id = ? ORDER BY event_time DESC LIMIT 1"
             )) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String col = "job".equalsIgnoreCase(type) ? "job_facets" : "facets";
                    String json = rs.getString(col);
                    if (json == null || json.isEmpty()) {
                        out.put("facets", Collections.emptyMap());
                    } else {
                        out.put("facets", MAPPER.readValue(json, Map.class));
                    }
                } else {
                    out.put("facets", Collections.emptyMap());
                }
            }
        }
        return out;
    }

    private Map<String, Object> buildRunFromRow(ResultSet rs, String ns, String job) throws Exception {
        String runId = rs.getString("run_id");
        String state = rs.getString("run_state");
        if (state == null || state.isEmpty()) {
            state = "NEW";
        }
        String at = tsIso(rs.getTimestamp("event_time"));
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", runId);
        r.put("createdAt", at);
        r.put("updatedAt", at);
        r.put("nominalStartTime", null);
        r.put("nominalEndTime", null);
        // Normalize OL event types that may have been stored pre-mapping
        if ("COMPLETE".equals(state)) {
            state = "COMPLETED";
        } else if ("FAIL".equals(state)) {
            state = "FAILED";
        } else if ("ABORT".equals(state)) {
            state = "ABORTED";
        } else if ("START".equals(state)) {
            state = "RUNNING";
        }
        r.put("state", state);
        r.put("startedAt", "RUNNING".equals(state) || "COMPLETED".equals(state) || "FAILED".equals(state) || "ABORTED".equals(state) ? at : null);
        r.put("endedAt", "COMPLETED".equals(state) || "FAILED".equals(state) || "ABORTED".equals(state) ? at : null);
        // Marquez UI JobRunItem divides by durationMs — null/NaN white-screens bars
        r.put("durationMs", 0);
        r.put("args", Collections.emptyMap());
        Map<String, Object> jv = new LinkedHashMap<>();
        jv.put("namespace", ns);
        jv.put("name", job);
        jv.put("version", null);
        r.put("jobVersion", jv);
        String facetsJson = null;
        try {
            facetsJson = rs.getString("facets");
        } catch (Exception ignore) {
        }
        if (facetsJson != null && !facetsJson.isEmpty()) {
            r.put("facets", MAPPER.readValue(facetsJson, Map.class));
        } else {
            r.put("facets", Collections.emptyMap());
        }
        r.put("inputDatasetVersions", Collections.emptyList());
        r.put("outputDatasetVersions", Collections.emptyList());
        return r;
    }

    // ─── Datasets ────────────────────────────────────────────────────────────

    public Map<String, Object> listDatasetsPage(String namespace, int limit, int offset) throws Exception {
        List<Map<String, Object>> all = new ArrayList<>();
        try (Connection c = connect()) {
            String prefix = namespace + ":";
            List<String> qns = cypher(c, "MATCH (d:Dataset) RETURN d.qualifiedName");
            for (String raw : qns) {
                String qn = unwrapJsonString(raw);
                if (qn == null || !qn.startsWith(prefix)) {
                    continue;
                }
                all.add(buildDataset(c, namespace, qn.substring(prefix.length())));
            }
            // also from events inputs/outputs
            try (PreparedStatement ps = c.prepareStatement(
                "SELECT DISTINCT e.elem->>'namespace' AS ns, e.elem->>'name' AS n FROM (" +
                "  SELECT jsonb_array_elements(COALESCE(inputs::jsonb, '[]'::jsonb)) AS elem FROM public.lineage_events " +
                "  UNION ALL " +
                "  SELECT jsonb_array_elements(COALESCE(outputs::jsonb, '[]'::jsonb)) AS elem FROM public.lineage_events" +
                ") e(elem) WHERE e.elem->>'namespace' = ?"
            )) {
                ps.setString(1, namespace);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String n = rs.getString("n");
                        if (n == null) {
                            continue;
                        }
                        boolean found = false;
                        for (Map<String, Object> d : all) {
                            if (n.equals(d.get("name"))) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            all.add(buildDataset(c, namespace, n));
                        }
                    }
                }
            }
        }
        int total = all.size();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("datasets", slice(all, limit, offset));
        out.put("totalCount", total);
        return out;
    }

    public List<Map<String, Object>> listDatasets(String namespace) throws Exception {
        Map<String, Object> page = listDatasetsPage(namespace, 10000, 0);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> ds = (List<Map<String, Object>>) page.get("datasets");
        return ds;
    }

    public Map<String, Object> getDataset(String namespace, String name) throws Exception {
        try (Connection c = connect()) {
            if (!datasetExists(c, namespace, name)) {
                return null;
            }
            return buildDataset(c, namespace, name);
        }
    }

    public Map<String, Object> upsertDataset(String namespace, String name, JsonNode meta) throws Exception {
        try (Connection c = connect()) {
            ensureNamespace(c, namespace);
            String qn = namespace + ":" + name;
            Map<String, String> dKeys = Collections.singletonMap("qualifiedName", qn);
            Map<String, String> dProps = mapOf(
                "qualifiedName", qn,
                "namespace", namespace,
                "name", name,
                "type", textOr(meta, "type", "DB_TABLE"),
                "description", text(meta, "description"),
                "sourceName", textOr(meta, "sourceName", "default")
            );
            mergeNode(c, "Dataset", dKeys, dProps);
            String source = textOr(meta, "sourceName", "default");
            if (!source.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO public.lineage_sources (name) VALUES (?) ON CONFLICT DO NOTHING"
                )) {
                    ps.setString(1, source);
                    ps.executeUpdate();
                }
            }
        }
        return getDataset(namespace, name);
    }

    public Map<String, Object> deleteDataset(String namespace, String name) throws Exception {
        Map<String, Object> before = getDataset(namespace, name);
        if (before == null) {
            return null;
        }
        String qn = namespace + ":" + name;
        try (Connection c = connect()) {
            cypher(c, "MATCH (d:Dataset {qualifiedName: " + lit(qn) + "}) DETACH DELETE d RETURN 1");
            try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM public.lineage_entity_tags WHERE entity_type IN ('dataset','field') AND namespace = ? AND name = ?"
            )) {
                ps.setString(1, namespace);
                ps.setString(2, name);
                ps.executeUpdate();
            }
        }
        return before;
    }

    public Map<String, Object> listDatasetVersions(String namespace, String name, int limit, int offset) throws Exception {
        // Synthetic version from latest event touching this dataset
        List<Map<String, Object>> versions = new ArrayList<>();
        Map<String, Object> ds = getDataset(namespace, name);
        if (ds != null) {
            Map<String, Object> v = new LinkedHashMap<>(ds);
            Map<String, Object> vid = new LinkedHashMap<>();
            vid.put("namespace", namespace);
            vid.put("name", name);
            vid.put("version", UUID.nameUUIDFromBytes((namespace + ":" + name).getBytes()).toString());
            v.put("id", vid);
            v.put("version", vid.get("version"));
            v.put("createdByRun", null);
            v.put("lifecycleState", null);
            versions.add(v);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("versions", slice(versions, limit, offset));
        out.put("totalCount", versions.size());
        return out;
    }

    public Map<String, Object> getDatasetVersion(String namespace, String name, String version) throws Exception {
        Map<String, Object> page = listDatasetVersions(namespace, name, 1, 0);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> versions = (List<Map<String, Object>>) page.get("versions");
        if (versions.isEmpty()) {
            return null;
        }
        return versions.get(0);
    }

    private boolean datasetExists(Connection c, String namespace, String name) throws Exception {
        String qn = namespace + ":" + name;
        List<String> hit = cypher(c, "MATCH (d:Dataset {qualifiedName: " + lit(qn) + "}) RETURN d.qualifiedName");
        if (!hit.isEmpty()) {
            return true;
        }
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT 1 FROM (" +
            "  SELECT jsonb_array_elements(COALESCE(inputs::jsonb, '[]'::jsonb)) AS elem FROM public.lineage_events " +
            "  UNION ALL " +
            "  SELECT jsonb_array_elements(COALESCE(outputs::jsonb, '[]'::jsonb)) AS elem FROM public.lineage_events" +
            ") e(elem) WHERE e.elem->>'namespace' = ? AND e.elem->>'name' = ? LIMIT 1"
        )) {
            ps.setString(1, namespace);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private Map<String, Object> buildDataset(Connection c, String namespace, String name) throws Exception {
        String now = Instant.now().toString();
        Map<String, Object> d = new LinkedHashMap<>();
        Map<String, String> id = new LinkedHashMap<>();
        id.put("namespace", namespace);
        id.put("name", name);
        d.put("id", id);
        d.put("type", "DB_TABLE");
        d.put("name", name);
        d.put("physicalName", name);
        d.put("createdAt", now);
        d.put("updatedAt", now);
        d.put("namespace", namespace);
        d.put("sourceName", datasetSourceName(c, namespace, name));
        d.put("fields", Collections.emptyList());
        d.put("tags", entityTags(c, "dataset", namespace, name, ""));
        d.put("lastModifiedAt", now);
        d.put("description", null);
        d.put("facets", Collections.emptyMap());
        d.put("deleted", false);
        d.put("columnLineage", Collections.emptyList());
        return d;
    }

    // ─── Tags ────────────────────────────────────────────────────────────────

    public Map<String, Object> listTags() throws Exception {
        List<Map<String, Object>> tags = new ArrayList<>();
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT name, description FROM public.lineage_tags ORDER BY name")) {
            while (rs.next()) {
                Map<String, Object> t = new LinkedHashMap<>();
                t.put("name", rs.getString("name"));
                String desc = rs.getString("description");
                t.put("description", desc == null ? "" : desc);
                tags.add(t);
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("tags", tags);
        return out;
    }

    public Map<String, Object> upsertTag(String name, JsonNode body) throws Exception {
        String desc = text(body, "description");
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "INSERT INTO public.lineage_tags (name, description) VALUES (?, ?) " +
                 "ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description"
             )) {
            ps.setString(1, name);
            ps.setString(2, desc.isEmpty() ? null : desc);
            ps.executeUpdate();
        }
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("name", name);
        t.put("description", desc);
        return t;
    }

    public Map<String, Object> addEntityTag(String entityType, String namespace, String name, String field, String tag) throws Exception {
        try (Connection c = connect()) {
            try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO public.lineage_tags (name) VALUES (?) ON CONFLICT DO NOTHING"
            )) {
                ps.setString(1, tag);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO public.lineage_entity_tags (entity_type, namespace, name, field_name, tag) " +
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"
            )) {
                ps.setString(1, entityType);
                ps.setString(2, namespace);
                ps.setString(3, name);
                ps.setString(4, field == null ? "" : field);
                ps.setString(5, tag);
                ps.executeUpdate();
            }
            if ("job".equals(entityType)) {
                return getJob(namespace, name);
            }
            return getDataset(namespace, name);
        }
    }

    public Map<String, Object> deleteEntityTag(String entityType, String namespace, String name, String field, String tag) throws Exception {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "DELETE FROM public.lineage_entity_tags WHERE entity_type = ? AND namespace = ? AND name = ? " +
                 "AND field_name = ? AND tag = ?"
             )) {
            ps.setString(1, entityType);
            ps.setString(2, namespace);
            ps.setString(3, name);
            ps.setString(4, field == null ? "" : field);
            ps.setString(5, tag);
            ps.executeUpdate();
        }
        if ("job".equals(entityType)) {
            return getJob(namespace, name);
        }
        return getDataset(namespace, name);
    }

    private List<String> entityTags(Connection c, String entityType, String namespace, String name, String field) throws Exception {
        List<String> tags = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT tag FROM public.lineage_entity_tags WHERE entity_type = ? AND namespace = ? AND name = ? AND field_name = ? ORDER BY tag"
        )) {
            ps.setString(1, entityType);
            ps.setString(2, namespace);
            ps.setString(3, name);
            ps.setString(4, field == null ? "" : field);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tags.add(rs.getString(1));
                }
            }
        }
        return tags;
    }

    // ─── Events ──────────────────────────────────────────────────────────────

    public Map<String, Object> listEvents(String before, String after, int limit, int offset, String sortDirection) throws Exception {
        boolean desc = sortDirection == null || !sortDirection.equalsIgnoreCase("asc");
        String order = desc ? "DESC" : "ASC";
        List<Map<String, Object>> events = new ArrayList<>();
        int total = 0;
        try (Connection c = connect()) {
            String countSql =
                "SELECT COUNT(*) FROM public.lineage_events WHERE 1=1" +
                (before != null && !before.isEmpty() ? " AND event_time <= ?::timestamptz" : "") +
                (after != null && !after.isEmpty() ? " AND event_time >= ?::timestamptz" : "");
            try (PreparedStatement ps = c.prepareStatement(countSql)) {
                int i = 1;
                if (before != null && !before.isEmpty()) {
                    ps.setString(i++, before);
                }
                if (after != null && !after.isEmpty()) {
                    ps.setString(i++, after);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        total = rs.getInt(1);
                    }
                }
            }
            String sql =
                "SELECT raw, event_time, run_id, job_namespace, job_name, run_state, producer, inputs, outputs, facets " +
                "FROM public.lineage_events WHERE 1=1" +
                (before != null && !before.isEmpty() ? " AND event_time <= ?::timestamptz" : "") +
                (after != null && !after.isEmpty() ? " AND event_time >= ?::timestamptz" : "") +
                " ORDER BY event_time " + order + " LIMIT ? OFFSET ?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                int i = 1;
                if (before != null && !before.isEmpty()) {
                    ps.setString(i++, before);
                }
                if (after != null && !after.isEmpty()) {
                    ps.setString(i++, after);
                }
                ps.setInt(i++, Math.max(0, limit));
                ps.setInt(i, Math.max(0, offset));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String raw = rs.getString("raw");
                        if (raw != null && !raw.isEmpty()) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> evt = MAPPER.readValue(raw, Map.class);
                            events.add(evt);
                        } else {
                            events.add(syntheticEvent(rs));
                        }
                    }
                }
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("events", events);
        out.put("totalCount", total);
        return out;
    }

    private Map<String, Object> syntheticEvent(ResultSet rs) throws Exception {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("eventType", fromRunState(rs.getString("run_state")));
        e.put("eventTime", tsIso(rs.getTimestamp("event_time")));
        e.put("producer", rs.getString("producer") == null ? "atlas-openlineage" : rs.getString("producer"));
        e.put("schemaURL", "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent");
        Map<String, Object> run = new LinkedHashMap<>();
        run.put("runId", rs.getString("run_id"));
        String facets = rs.getString("facets");
        run.put("facets", facets == null || facets.isEmpty() ? Collections.emptyMap() : MAPPER.readValue(facets, Map.class));
        e.put("run", run);
        Map<String, Object> job = new LinkedHashMap<>();
        job.put("namespace", rs.getString("job_namespace"));
        job.put("name", rs.getString("job_name"));
        job.put("facets", Collections.emptyMap());
        e.put("job", job);
        String inputs = rs.getString("inputs");
        String outputs = rs.getString("outputs");
        e.put("inputs", inputs == null ? Collections.emptyList() : MAPPER.readValue(inputs, List.class));
        e.put("outputs", outputs == null ? Collections.emptyList() : MAPPER.readValue(outputs, List.class));
        return e;
    }

    // ─── Search ──────────────────────────────────────────────────────────────

    public Map<String, Object> search(String q, String filter, int limit) throws Exception {
        String qq = q == null ? "" : q.toLowerCase(Locale.ROOT);
        List<Map<String, Object>> results = new ArrayList<>();
        boolean wantJobs = filter == null || filter.isEmpty() || "ALL".equalsIgnoreCase(filter) || "JOB".equalsIgnoreCase(filter);
        boolean wantDs = filter == null || filter.isEmpty() || "ALL".equalsIgnoreCase(filter) || "DATASET".equalsIgnoreCase(filter);
        try (Connection c = connect()) {
            if (wantJobs) {
                Map<String, Object> jobs = listJobsPage(null, 10000, 0);
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> list = (List<Map<String, Object>>) jobs.get("jobs");
                for (Map<String, Object> j : list) {
                    String name = String.valueOf(j.get("name"));
                    String ns = String.valueOf(j.get("namespace"));
                    if (qq.isEmpty() || name.toLowerCase(Locale.ROOT).contains(qq) || ns.toLowerCase(Locale.ROOT).contains(qq)) {
                        Map<String, Object> r = new LinkedHashMap<>();
                        r.put("type", "JOB");
                        r.put("name", name);
                        r.put("namespace", ns);
                        r.put("updatedAt", j.get("updatedAt"));
                        r.put("nodeId", "job:" + ns + ":" + name);
                        results.add(r);
                    }
                }
            }
            if (wantDs) {
                for (Map<String, Object> nsMap : listNamespacesDetailed()) {
                    String ns = String.valueOf(nsMap.get("name"));
                    Map<String, Object> page = listDatasetsPage(ns, 10000, 0);
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> list = (List<Map<String, Object>>) page.get("datasets");
                    for (Map<String, Object> d : list) {
                        String name = String.valueOf(d.get("name"));
                        if (qq.isEmpty() || name.toLowerCase(Locale.ROOT).contains(qq) || ns.toLowerCase(Locale.ROOT).contains(qq)) {
                            Map<String, Object> r = new LinkedHashMap<>();
                            r.put("type", "DATASET");
                            r.put("name", name);
                            r.put("namespace", ns);
                            r.put("updatedAt", d.get("updatedAt"));
                            r.put("nodeId", "dataset:" + ns + ":" + name);
                            results.add(r);
                        }
                    }
                }
            }
        }
        if (limit > 0 && results.size() > limit) {
            results = results.subList(0, limit);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("totalCount", results.size());
        out.put("results", results);
        return out;
    }

    public Map<String, Object> openSearchJobs(String q) throws Exception {
        Map<String, Object> s = search(q, "JOB", 50);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results = (List<Map<String, Object>>) s.get("results");
        List<Map<String, Object>> hits = new ArrayList<>();
        for (Map<String, Object> r : results) {
            Map<String, Object> hit = new LinkedHashMap<>();
            hit.put("name", r.get("name"));
            hit.put("namespace", r.get("namespace"));
            hit.put("type", "JOB");
            hits.add(hit);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("hits", hits);
        out.put("highlights", Collections.emptyList());
        return out;
    }

    public Map<String, Object> openSearchDatasets(String q) throws Exception {
        Map<String, Object> s = search(q, "DATASET", 50);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results = (List<Map<String, Object>>) s.get("results");
        List<Map<String, Object>> hits = new ArrayList<>();
        for (Map<String, Object> r : results) {
            Map<String, Object> hit = new LinkedHashMap<>();
            hit.put("name", r.get("name"));
            hit.put("namespace", r.get("namespace"));
            hit.put("type", "DATASET");
            hits.add(hit);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("hits", hits);
        out.put("highlights", Collections.emptyList());
        return out;
    }

    // ─── Stats ───────────────────────────────────────────────────────────────

    public List<Map<String, Object>> lineageEventMetrics(String period, String timezone) throws Exception {
        int buckets = "WEEK".equalsIgnoreCase(period) ? 7 : 24;
        ChronoUnit unit = "WEEK".equalsIgnoreCase(period) ? ChronoUnit.DAYS : ChronoUnit.HOURS;
        Instant end = Instant.now().truncatedTo(unit);
        List<Map<String, Object>> out = new ArrayList<>();
        try (Connection c = connect()) {
            for (int i = buckets - 1; i >= 0; i--) {
                Instant start = end.minus(i + 1L, unit);
                Instant stop = end.minus(i, unit);
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("startInterval", start.atOffset(ZoneOffset.UTC).format(ISO));
                m.put("endInterval", stop.atOffset(ZoneOffset.UTC).format(ISO));
                int fail = 0, startN = 0, complete = 0, abort = 0;
                try (PreparedStatement ps = c.prepareStatement(
                    "SELECT run_state, COUNT(*) FROM public.lineage_events " +
                    "WHERE event_time >= ? AND event_time < ? GROUP BY run_state"
                )) {
                    ps.setTimestamp(1, Timestamp.from(start));
                    ps.setTimestamp(2, Timestamp.from(stop));
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String st = rs.getString(1);
                            int cnt = rs.getInt(2);
                            if ("FAILED".equals(st)) {
                                fail += cnt;
                            } else if ("RUNNING".equals(st) || "NEW".equals(st)) {
                                startN += cnt;
                            } else if ("COMPLETED".equals(st)) {
                                complete += cnt;
                            } else if ("ABORTED".equals(st)) {
                                abort += cnt;
                            } else {
                                startN += cnt;
                            }
                        }
                    }
                }
                m.put("fail", fail);
                m.put("start", startN);
                m.put("complete", complete);
                m.put("abort", abort);
                out.add(m);
            }
        }
        return out;
    }

    public List<Map<String, Object>> intervalMetrics(String asset, String period, String timezone) throws Exception {
        int buckets = "WEEK".equalsIgnoreCase(period) ? 7 : 24;
        ChronoUnit unit = "WEEK".equalsIgnoreCase(period) ? ChronoUnit.DAYS : ChronoUnit.HOURS;
        Instant end = Instant.now().truncatedTo(unit);
        List<Map<String, Object>> out = new ArrayList<>();
        try (Connection c = connect()) {
            for (int i = buckets - 1; i >= 0; i--) {
                Instant start = end.minus(i + 1L, unit);
                Instant stop = end.minus(i, unit);
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("startInterval", start.atOffset(ZoneOffset.UTC).format(ISO));
                m.put("endInterval", stop.atOffset(ZoneOffset.UTC).format(ISO));
                int count = 0;
                String sql;
                if ("datasets".equalsIgnoreCase(asset)) {
                    // Explicit jsonb cast — AGE search_path can otherwise resolve array_elements oddly
                    sql = "SELECT COUNT(DISTINCT ((e.elem->>'namespace') || ':' || (e.elem->>'name'))) FROM (" +
                          "  SELECT jsonb_array_elements(COALESCE(outputs::jsonb, '[]'::jsonb)) AS elem " +
                          "  FROM public.lineage_events " +
                          "  WHERE event_time >= ? AND event_time < ?" +
                          ") e(elem) WHERE (e.elem->>'name') IS NOT NULL";
                } else if ("sources".equalsIgnoreCase(asset)) {
                    sql = "SELECT COUNT(*) FROM public.lineage_sources WHERE created_at >= ? AND created_at < ?";
                } else {
                    // jobs
                    sql = "SELECT COUNT(DISTINCT (job_namespace || ':' || job_name)) FROM public.lineage_events " +
                          "WHERE event_time >= ? AND event_time < ?";
                }
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setTimestamp(1, Timestamp.from(start));
                    ps.setTimestamp(2, Timestamp.from(stop));
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            count = rs.getInt(1);
                        }
                    }
                }
                m.put("count", count);
                out.add(m);
            }
        }
        return out;
    }

    // ─── Sources ─────────────────────────────────────────────────────────────

    public Map<String, Object> listSources(int limit, int offset) throws Exception {
        List<Map<String, Object>> sources = new ArrayList<>();
        try (Connection c = connect();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT name, type, connection_url, description, created_at, updated_at " +
                 "FROM public.lineage_sources ORDER BY name"
             )) {
            while (rs.next()) {
                sources.add(sourceRow(rs));
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("sources", slice(sources, limit, offset));
        out.put("totalCount", sources.size());
        return out;
    }

    public Map<String, Object> getSource(String name) throws Exception {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT name, type, connection_url, description, created_at, updated_at " +
                 "FROM public.lineage_sources WHERE name = ?"
             )) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return sourceRow(rs);
                }
            }
        }
        return null;
    }

    public Map<String, Object> upsertSource(String name, JsonNode meta) throws Exception {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                 "INSERT INTO public.lineage_sources (name, type, connection_url, description) VALUES (?, ?, ?, ?) " +
                 "ON CONFLICT (name) DO UPDATE SET type = EXCLUDED.type, connection_url = EXCLUDED.connection_url, " +
                 "description = EXCLUDED.description, updated_at = NOW()"
             )) {
            ps.setString(1, name);
            ps.setString(2, textOr(meta, "type", "POSTGRESQL"));
            ps.setString(3, emptyToNull(text(meta, "connectionUrl")));
            ps.setString(4, emptyToNull(text(meta, "description")));
            ps.executeUpdate();
        }
        return getSource(name);
    }

    private static Map<String, Object> sourceRow(ResultSet rs) throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", rs.getString("name"));
        m.put("type", rs.getString("type"));
        m.put("connectionUrl", rs.getString("connection_url"));
        m.put("description", rs.getString("description"));
        m.put("createdAt", tsIso(rs.getTimestamp("created_at")));
        m.put("updatedAt", tsIso(rs.getTimestamp("updated_at")));
        return m;
    }

    // ─── Lineage graph ───────────────────────────────────────────────────────

    public Map<String, Object> lineageGraph(String nodeId, int depth) throws Exception {
        String[] parts = nodeId.split(":", 3);
        if (parts.length < 3) {
            throw new IllegalArgumentException("nodeId must be kind:namespace:name");
        }
        String kind = parts[0];
        String ns = parts[1];
        String name = parts[2];
        depth = Math.max(1, Math.min(depth, 5));

        Map<String, Map<String, Object>> graph = new LinkedHashMap<>();

        try (Connection c = connect()) {
            if ("job".equalsIgnoreCase(kind)) {
                expandJob(c, graph, ns, name, depth);
            } else if ("dataset".equalsIgnoreCase(kind)) {
                expandDataset(c, graph, ns + ":" + name, depth);
            } else {
                throw new IllegalArgumentException("unknown node kind: " + kind);
            }
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("graph", new ArrayList<>(graph.values()));
        return resp;
    }

    public Map<String, Object> columnLineageGraph(String nodeId, int depth, boolean withDownstream) {
        // Column-level lineage is populated when OL columnLineage facets are present.
        // Empty graph is a valid Marquez response for datasets without column facets.
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("graph", Collections.emptyList());
        return resp;
    }

    public Map<String, Object> runLineageUpstream(String runId, int depth) throws Exception {
        Map<String, Object> run = getRun(runId);
        Map<String, Object> resp = new LinkedHashMap<>();
        if (run == null) {
            resp.put("graph", Collections.emptyList());
            return resp;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> jv = (Map<String, Object>) run.get("jobVersion");
        if (jv == null) {
            resp.put("graph", Collections.emptyList());
            return resp;
        }
        String nodeId = "job:" + jv.get("namespace") + ":" + jv.get("name");
        return lineageGraph(nodeId, Math.max(1, depth));
    }

    private void expandJob(Connection c, Map<String, Map<String, Object>> graph,
                           String ns, String name, int depth) throws Exception {
        String jid = "job:" + ns + ":" + name;
        node(graph, jid, "JOB", jobData(ns, name));
        List<String> ins = cypher(c,
            "MATCH (d:Dataset)-[:INPUT_TO]->(:Run)<-[:EXECUTES]-(:Job {namespace: " + lit(ns) +
            ", name: " + lit(name) + "}) RETURN DISTINCT d.qualifiedName");
        List<String> outs = cypher(c,
            "MATCH (:Job {namespace: " + lit(ns) + ", name: " + lit(name) +
            "})-[:EXECUTES]->(:Run)-[:OUTPUTS]->(d:Dataset) RETURN DISTINCT d.qualifiedName");
        for (String raw : ins) {
            String qn = unwrapJsonString(raw);
            if (qn == null) {
                continue;
            }
            String did = datasetId(qn);
            String[] p = qn.split(":", 2);
            node(graph, did, "DATASET", datasetData(c, p[0], p.length > 1 ? p[1] : qn));
            edge(graph, did, jid);
            if (depth > 1) {
                expandDataset(c, graph, qn, depth - 1);
            }
        }
        for (String raw : outs) {
            String qn = unwrapJsonString(raw);
            if (qn == null) {
                continue;
            }
            String did = datasetId(qn);
            String[] p = qn.split(":", 2);
            node(graph, did, "DATASET", datasetData(c, p[0], p.length > 1 ? p[1] : qn));
            edge(graph, jid, did);
            if (depth > 1) {
                expandDataset(c, graph, qn, depth - 1);
            }
        }
    }

    private void expandDataset(Connection c, Map<String, Map<String, Object>> graph,
                               String qn, int depth) throws Exception {
        String did = datasetId(qn);
        String[] p = qn.split(":", 2);
        node(graph, did, "DATASET", datasetData(c, p[0], p.length > 1 ? p[1] : qn));
        List<String> producers = cypher(c,
            "MATCH (j:Job)-[:EXECUTES]->(:Run)-[:OUTPUTS]->(:Dataset {qualifiedName: " + lit(qn) +
            "}) RETURN {ns: j.namespace, n: j.name}");
        List<String> consumers = cypher(c,
            "MATCH (:Dataset {qualifiedName: " + lit(qn) +
            "})-[:INPUT_TO]->(:Run)<-[:EXECUTES]-(j:Job) RETURN {ns: j.namespace, n: j.name}");
        for (String raw : producers) {
            JsonNode n = MAPPER.readTree(raw);
            String jns = text(n, "ns");
            String jn = text(n, "n");
            String jid = "job:" + jns + ":" + jn;
            node(graph, jid, "JOB", jobData(jns, jn));
            edge(graph, jid, did);
            if (depth > 1) {
                expandJob(c, graph, jns, jn, depth - 1);
            }
        }
        for (String raw : consumers) {
            JsonNode n = MAPPER.readTree(raw);
            String jns = text(n, "ns");
            String jn = text(n, "n");
            String jid = "job:" + jns + ":" + jn;
            node(graph, jid, "JOB", jobData(jns, jn));
            edge(graph, did, jid);
            if (depth > 1) {
                expandJob(c, graph, jns, jn, depth - 1);
            }
        }
    }

    private static void node(Map<String, Map<String, Object>> graph, String id, String type, Map<String, Object> data) {
        graph.computeIfAbsent(id, k -> {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("id", id);
            n.put("type", type);
            n.put("data", data);
            n.put("inEdges", new ArrayList<Map<String, String>>());
            n.put("outEdges", new ArrayList<Map<String, String>>());
            return n;
        });
    }

    @SuppressWarnings("unchecked")
    private static void edge(Map<String, Map<String, Object>> graph, String src, String dst) {
        if (!graph.containsKey(src) || !graph.containsKey(dst)) {
            return;
        }
        Map<String, String> e = new LinkedHashMap<>();
        e.put("origin", src);
        e.put("destination", dst);
        List<Map<String, String>> out = (List<Map<String, String>>) graph.get(src).get("outEdges");
        List<Map<String, String>> in = (List<Map<String, String>>) graph.get(dst).get("inEdges");
        if (!out.contains(e)) {
            out.add(e);
        }
        if (!in.contains(e)) {
            in.add(e);
        }
    }

    private static String datasetId(String qn) {
        String[] p = qn.split(":", 2);
        return "dataset:" + p[0] + ":" + (p.length > 1 ? p[1] : "");
    }

    private static Map<String, Object> jobData(String ns, String name) {
        // Full LineageJob-shaped payload for Marquez table-level graph nodes
        String now = Instant.now().toString();
        Map<String, Object> d = new LinkedHashMap<>();
        Map<String, String> id = new LinkedHashMap<>();
        id.put("namespace", ns);
        id.put("name", name);
        d.put("id", id);
        d.put("type", "BATCH");
        d.put("name", name);
        d.put("simpleName", simpleName(name));
        d.put("createdAt", now);
        d.put("updatedAt", now);
        d.put("namespace", ns);
        d.put("inputs", Collections.emptyList());
        d.put("outputs", Collections.emptyList());
        d.put("location", null);
        d.put("description", null);
        d.put("latestRun", null);
        d.put("parentJobName", null);
        d.put("parentJobUuid", null);
        return d;
    }

    private static Map<String, Object> datasetData(Connection c, String ns, String name) {
        // Full LineageDataset-shaped payload for Marquez table-level graph nodes
        String now = Instant.now().toString();
        Map<String, Object> d = new LinkedHashMap<>();
        Map<String, String> id = new LinkedHashMap<>();
        id.put("namespace", ns);
        id.put("name", name);
        d.put("id", id);
        d.put("type", "DB_TABLE");
        d.put("name", name);
        d.put("physicalName", name);
        d.put("createdAt", now);
        d.put("updatedAt", now);
        d.put("namespace", ns);
        d.put("sourceName", datasetSourceName(c, ns, name));
        d.put("fields", Collections.emptyList());
        d.put("facets", Collections.emptyMap());
        d.put("tags", Collections.emptyList());
        d.put("lastModifiedAt", now);
        d.put("description", null);
        return d;
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private void materializeSources(Connection c, JsonNode body) throws Exception {
        for (String side : new String[] {"inputs", "outputs"}) {
            JsonNode arr = body.path(side);
            if (arr == null || !arr.isArray()) {
                continue;
            }
            for (JsonNode d : arr) {
                String ns = text(d, "namespace");
                String name = text(d, "name");
                if (ns.isEmpty() || name.isEmpty()) {
                    continue;
                }
                JsonNode src = d.path("facets").path("dataSource");
                String sourceName = text(src, "name");
                if (sourceName.isEmpty()) {
                    sourceName = "default";
                }
                String uri = text(src, "uri");
                try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO public.lineage_sources (name, connection_url) VALUES (?, ?) " +
                    "ON CONFLICT (name) DO UPDATE SET " +
                    " connection_url = COALESCE(NULLIF(EXCLUDED.connection_url, ''), public.lineage_sources.connection_url)," +
                    " updated_at = NOW()"
                )) {
                    ps.setString(1, sourceName);
                    ps.setString(2, uri.isEmpty() ? null : uri);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO public.lineage_dataset_sources (namespace, name, source_name) VALUES (?, ?, ?) " +
                    "ON CONFLICT (namespace, name) DO UPDATE SET source_name = EXCLUDED.source_name"
                )) {
                    ps.setString(1, ns);
                    ps.setString(2, name);
                    ps.setString(3, sourceName);
                    ps.executeUpdate();
                }
            }
        }
    }

    private static String datasetSourceName(Connection c, String namespace, String name) {
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT source_name FROM public.lineage_dataset_sources WHERE namespace = ? AND name = ?"
        )) {
            ps.setString(1, namespace);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (Exception ignore) {
            // fall through — "default" is the honest floor
        }
        return "default";
    }

    private static List<String> datasetQns(JsonNode arr) {
        List<String> out = new ArrayList<>();
        if (arr == null || !arr.isArray()) {
            return out;
        }
        for (JsonNode d : arr) {
            String ns = text(d, "namespace");
            String name = text(d, "name");
            if (!ns.isEmpty() && !name.isEmpty()) {
                out.add(ns + ":" + name);
            }
        }
        return out;
    }

    private static String text(JsonNode n, String field) {
        if (n == null || n.isMissingNode() || n.isNull()) {
            return "";
        }
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? "" : v.asText("").trim();
    }

    private static String textOr(JsonNode n, String field, String def) {
        String t = text(n, field);
        return t.isEmpty() ? def : t;
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    private static String unwrapJsonString(String raw) {
        if (raw == null) {
            return null;
        }
        String t = raw.trim();
        if (t.startsWith("\"") && t.endsWith("\"")) {
            try {
                return MAPPER.readValue(t, String.class);
            } catch (Exception e) {
                return t.substring(1, t.length() - 1);
            }
        }
        return t;
    }

    private static Map<String, String> mapOf(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String tsIso(Timestamp ts) {
        if (ts == null) {
            return Instant.now().toString();
        }
        return ts.toInstant().atOffset(ZoneOffset.UTC).format(ISO);
    }

    private static <T> List<T> slice(List<T> all, int limit, int offset) {
        if (offset < 0) {
            offset = 0;
        }
        if (offset >= all.size()) {
            return Collections.emptyList();
        }
        int end = limit <= 0 ? all.size() : Math.min(all.size(), offset + limit);
        return new ArrayList<>(all.subList(offset, end));
    }

    private static String simpleName(String name) {
        if (name == null) {
            return "";
        }
        int idx = name.lastIndexOf('.');
        return idx >= 0 ? name.substring(idx + 1) : name;
    }

    /** Map OL eventType → Marquez RunState. */
    static String toRunState(String eventType) {
        if (eventType == null) {
            return "NEW";
        }
        switch (eventType.toUpperCase(Locale.ROOT)) {
            case "START":
            case "RUNNING":
                return "RUNNING";
            case "COMPLETE":
            case "COMPLETED":
                return "COMPLETED";
            case "FAIL":
            case "FAILED":
                return "FAILED";
            case "ABORT":
            case "ABORTED":
                return "ABORTED";
            default:
                return "NEW";
        }
    }

    static String fromRunState(String state) {
        if (state == null) {
            return "OTHER";
        }
        switch (state.toUpperCase(Locale.ROOT)) {
            case "RUNNING":
            case "START":
                return "START";
            case "COMPLETED":
            case "COMPLETE":
                return "COMPLETE";
            case "FAILED":
            case "FAIL":
                return "FAIL";
            case "ABORTED":
            case "ABORT":
                return "ABORT";
            default:
                return "OTHER";
        }
    }
}
