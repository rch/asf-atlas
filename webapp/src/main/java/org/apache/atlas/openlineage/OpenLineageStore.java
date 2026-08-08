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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * AGE + event-log store for OpenLineage / Marquez-compat surface.
 * Graph name defaults to {@code signals_ol} on the Atlas JDBC database (signals).
 * Labels: Job, Run, Dataset; edges EXECUTES, INPUT_TO, OUTPUTS.
 */
@Service
public class OpenLineageStore {
    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
                " raw JSONB," +
                " created_at TIMESTAMPTZ DEFAULT NOW()" +
                ")"
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
        String eventType = body.path("eventType").asText(body.path("event_type").asText("OTHER")).toUpperCase();
        String eventTime = body.path("eventTime").asText(body.path("event_time").asText(""));
        String producer = body.path("producer").asText("");

        List<String> inputs = datasetQns(body.path("inputs"));
        List<String> outputs = datasetQns(body.path("outputs"));

        try (Connection c = connect()) {
            c.setAutoCommit(true);
            try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO public.lineage_events (" +
                " event_type, event_time, run_id, job_namespace, job_name, run_state, producer," +
                " inputs, outputs, facets, raw) VALUES (" +
                " 'RUN_EVENT', COALESCE(?::timestamptz, NOW()), ?, ?, ?, ?, ?," +
                " ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb)"
            )) {
                ps.setString(1, eventTime.isEmpty() ? null : eventTime);
                ps.setString(2, runId);
                ps.setString(3, ns);
                ps.setString(4, name);
                ps.setString(5, eventType);
                ps.setString(6, producer.isEmpty() ? null : producer);
                ps.setString(7, MAPPER.writeValueAsString(body.path("inputs")));
                ps.setString(8, MAPPER.writeValueAsString(body.path("outputs")));
                ps.setString(9, MAPPER.writeValueAsString(run.path("facets")));
                ps.setString(10, MAPPER.writeValueAsString(body));
                ps.executeUpdate();
            }

            Map<String, String> jobKeys = mapOf("namespace", ns, "name", name);
            mergeNode(c, "Job", jobKeys, jobKeys);
            Map<String, String> runKeys = Collections.singletonMap("run_id", runId);
            Map<String, String> runProps = mapOf(
                "run_id", runId,
                "eventType", eventType,
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

    public List<String> listNamespaces() throws Exception {
        try (Connection c = connect()) {
            List<String> raw = cypher(c, "MATCH (j:Job) RETURN DISTINCT j.namespace");
            Set<String> ns = new LinkedHashSet<>();
            for (String r : raw) {
                String v = unwrapJsonString(r);
                if (v != null && !v.isEmpty()) {
                    ns.add(v);
                }
            }
            return new ArrayList<>(ns);
        }
    }

    public List<Map<String, Object>> listJobs(String namespace) throws Exception {
        try (Connection c = connect()) {
            List<String> names = cypher(c,
                "MATCH (j:Job {namespace: " + lit(namespace) + "}) RETURN j.name");
            List<Map<String, Object>> jobs = new ArrayList<>();
            for (String nraw : names) {
                String name = unwrapJsonString(nraw);
                if (name == null) {
                    continue;
                }
                Map<String, Object> job = new LinkedHashMap<>();
                Map<String, String> id = new LinkedHashMap<>();
                id.put("namespace", namespace);
                id.put("name", name);
                job.put("id", id);
                job.put("name", name);
                job.put("namespace", namespace);
                job.put("type", "BATCH");
                job.put("simple_name", name);
                job.put("latestRun", null);
                jobs.add(job);
            }
            return jobs;
        }
    }

    public List<Map<String, Object>> listRuns(String namespace, String job) throws Exception {
        try (Connection c = connect()) {
            String q =
                "MATCH (:Job {namespace: " + lit(namespace) + ", name: " + lit(job) + "})" +
                "-[:EXECUTES]->(r:Run) RETURN r.run_id, r.eventType, r.eventTime";
            // AGE single-column return — use object form
            List<String> rows = cypher(c,
                "MATCH (:Job {namespace: " + lit(namespace) + ", name: " + lit(job) + "})" +
                "-[:EXECUTES]->(r:Run) " +
                "RETURN {id: r.run_id, state: r.eventType, at: r.eventTime}");
            List<Map<String, Object>> runs = new ArrayList<>();
            for (String row : rows) {
                JsonNode n = MAPPER.readTree(row);
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("id", text(n, "id"));
                r.put("state", text(n, "state"));
                r.put("createdAt", text(n, "at"));
                Map<String, String> jv = new LinkedHashMap<>();
                jv.put("namespace", namespace);
                jv.put("name", job);
                r.put("jobVersion", jv);
                runs.add(r);
            }
            return runs;
        }
    }

    public List<Map<String, Object>> listDatasets(String namespace) throws Exception {
        try (Connection c = connect()) {
            List<String> qns = cypher(c, "MATCH (d:Dataset) RETURN d.qualifiedName");
            List<Map<String, Object>> out = new ArrayList<>();
            String prefix = namespace + ":";
            for (String raw : qns) {
                String qn = unwrapJsonString(raw);
                if (qn == null || !qn.startsWith(prefix)) {
                    continue;
                }
                String name = qn.substring(prefix.length());
                Map<String, Object> d = new LinkedHashMap<>();
                Map<String, String> id = new LinkedHashMap<>();
                id.put("namespace", namespace);
                id.put("name", name);
                d.put("id", id);
                d.put("name", name);
                d.put("namespace", namespace);
                d.put("type", "DB_TABLE");
                out.add(d);
            }
            return out;
        }
    }

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
            if ("job".equals(kind)) {
                expandJob(c, graph, ns, name, depth);
            } else if ("dataset".equals(kind)) {
                expandDataset(c, graph, ns + ":" + name, depth);
            } else {
                throw new IllegalArgumentException("unknown node kind: " + kind);
            }
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("graph", new ArrayList<>(graph.values()));
        return resp;
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
            node(graph, did, "DATASET", datasetData(p[0], p.length > 1 ? p[1] : qn));
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
            node(graph, did, "DATASET", datasetData(p[0], p.length > 1 ? p[1] : qn));
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
        node(graph, did, "DATASET", datasetData(p[0], p.length > 1 ? p[1] : qn));
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
        Map<String, Object> d = new LinkedHashMap<>();
        Map<String, String> id = new LinkedHashMap<>();
        id.put("namespace", ns);
        id.put("name", name);
        d.put("id", id);
        d.put("name", name);
        d.put("namespace", ns);
        d.put("type", "BATCH");
        return d;
    }

    private static Map<String, Object> datasetData(String ns, String name) {
        Map<String, Object> d = new LinkedHashMap<>();
        Map<String, String> id = new LinkedHashMap<>();
        id.put("namespace", ns);
        id.put("name", name);
        d.put("id", id);
        d.put("name", name);
        d.put("namespace", ns);
        d.put("type", "DB_TABLE");
        return d;
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
        if (n == null || n.isMissingNode()) {
            return "";
        }
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? "" : v.asText("").trim();
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
}
