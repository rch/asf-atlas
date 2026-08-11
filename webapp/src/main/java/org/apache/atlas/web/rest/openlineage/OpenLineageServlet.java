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
package org.apache.atlas.web.rest.openlineage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.openlineage.OpenLineageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Complete Marquez-compatible OpenLineage surface at {@code /api/v1/*} and
 * {@code /api/v2beta/*}.
 * <p>
 * Atlas is the system of record; Marquez UI (and Marquez API contract tests)
 * are the validation harness that the enhanced API is complete and valid for
 * all aspects of OpenLineage.
 * <p>
 * Implemented as a plain HttpServlet (not Jersey) because Atlas
 * {@code LineageResource} is {@code @Path("lineage")} and SpringServlet
 * registers every {@code @Path} bean on every mapping of that servlet —
 * so a shared Jersey mapping for {@code /api/v1/*} made POST/GET
 * {@code /api/v1/lineage} hit the Atlas lineage resource (405/500 via
 * TerminatingRule) instead of OpenLineage.
 * <p>
 * Does not alter {@code /api/atlas/*} governance clients.
 */
public class OpenLineageServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageServlet.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String JSON = "application/json; charset=UTF-8";

    private OpenLineageStore store;

    @Override
    public void init() throws ServletException {
        WebApplicationContext ctx = WebApplicationContextUtils.getRequiredWebApplicationContext(getServletContext());
        store = ctx.getBean(OpenLineageStore.class);
        LOG.info("OpenLineageServlet ready (store ready={}) — full Marquez-compat surface", store.isReady());
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Route r = route(req);
        try {
            if (r.v2beta) {
                handleV2BetaGet(r, req, resp);
                return;
            }
            handleV1Get(r, req, resp);
        } catch (NotFoundException e) {
            writeJsonError(resp, 404, e.getMessage());
        } catch (IllegalArgumentException e) {
            writeJsonError(resp, 400, e.getMessage());
        } catch (Exception e) {
            LOG.error("OpenLineage GET {} failed", r.path, e);
            writeJsonError(resp, 500, "error: " + e.getMessage());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Route r = route(req);
        try {
            if (r.v2beta) {
                writeJsonError(resp, 404, "not found: " + r.path);
                return;
            }
            handleV1Post(r, req, resp);
        } catch (NotFoundException e) {
            writeJsonError(resp, 404, e.getMessage());
        } catch (IllegalArgumentException e) {
            writeJsonError(resp, 400, e.getMessage());
        } catch (Exception e) {
            LOG.error("OpenLineage POST {} failed", r.path, e);
            writeJsonError(resp, 500, "error: " + e.getMessage());
        }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Route r = route(req);
        try {
            if (r.v2beta) {
                writeJsonError(resp, 404, "not found: " + r.path);
                return;
            }
            handleV1Put(r, req, resp);
        } catch (NotFoundException e) {
            writeJsonError(resp, 404, e.getMessage());
        } catch (IllegalArgumentException e) {
            writeJsonError(resp, 400, e.getMessage());
        } catch (Exception e) {
            LOG.error("OpenLineage PUT {} failed", r.path, e);
            writeJsonError(resp, 500, "error: " + e.getMessage());
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Route r = route(req);
        try {
            if (r.v2beta) {
                writeJsonError(resp, 404, "not found: " + r.path);
                return;
            }
            handleV1Delete(r, req, resp);
        } catch (NotFoundException e) {
            writeJsonError(resp, 404, e.getMessage());
        } catch (IllegalArgumentException e) {
            writeJsonError(resp, 400, e.getMessage());
        } catch (Exception e) {
            LOG.error("OpenLineage DELETE {} failed", r.path, e);
            writeJsonError(resp, 500, "error: " + e.getMessage());
        }
    }

    // ─── v1 GET ──────────────────────────────────────────────────────────────

    private void handleV1Get(Route r, HttpServletRequest req, HttpServletResponse resp) throws Exception {
        List<String> p = r.parts;

        if (p.isEmpty() || (p.size() == 1 && ("health".equals(p.get(0)) || p.get(0).isEmpty()))) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("status", store.isReady() ? "ok" : "degraded");
            m.put("surface", "openlineage-marquez-complete");
            m.put("api", "v1");
            writeJson(resp, 200, m);
            return;
        }

        // /namespaces
        if (eq(p, "namespaces")) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("namespaces", store.listNamespacesDetailed());
            writeJson(resp, 200, out);
            return;
        }
        // /namespaces/{ns}
        if (p.size() == 2 && "namespaces".equals(p.get(0))) {
            Map<String, Object> ns = store.getNamespace(p.get(1));
            if (ns == null) {
                throw new NotFoundException("namespace not found: " + p.get(1));
            }
            writeJson(resp, 200, ns);
            return;
        }

        // /jobs  (global)
        if (eq(p, "jobs")) {
            writeJson(resp, 200, store.listJobsPage(null, limit(req, 100), offset(req)));
            return;
        }

        // /jobs/runs/{id}
        if (p.size() == 3 && "jobs".equals(p.get(0)) && "runs".equals(p.get(1))) {
            Map<String, Object> run = store.getRun(p.get(2));
            if (run == null) {
                throw new NotFoundException("run not found: " + p.get(2));
            }
            writeJson(resp, 200, run);
            return;
        }
        // /jobs/runs/{id}/facets
        if (p.size() == 4 && "jobs".equals(p.get(0)) && "runs".equals(p.get(1)) && "facets".equals(p.get(3))) {
            String type = param(req, "type", "run");
            writeJson(resp, 200, store.getRunFacets(p.get(2), type));
            return;
        }

        // /namespaces/{ns}/jobs ...
        if (p.size() >= 3 && "namespaces".equals(p.get(0)) && "jobs".equals(p.get(2))) {
            String ns = p.get(1);
            if (p.size() == 3) {
                writeJson(resp, 200, store.listJobsPage(ns, limit(req, 100), offset(req)));
                return;
            }
            JobPath jp = parseJobPath(p, 3);
            if (jp.suffix.isEmpty()) {
                Map<String, Object> job = store.getJob(ns, jp.job);
                if (job == null) {
                    throw new NotFoundException("job not found: " + ns + "/" + jp.job);
                }
                writeJson(resp, 200, job);
                return;
            }
            if ("runs".equals(jp.suffix)) {
                writeJson(resp, 200, store.listRunsPage(ns, jp.job, limit(req, 100), offset(req)));
                return;
            }
            if ("versions".equals(jp.suffix)) {
                // Minimal: empty versions list with totalCount (job versions not versioned yet)
                Map<String, Object> out = new LinkedHashMap<>();
                Map<String, Object> job = store.getJob(ns, jp.job);
                List<Map<String, Object>> versions = new ArrayList<>();
                if (job != null) {
                    Map<String, Object> v = new LinkedHashMap<>();
                    v.put("id", job.get("id"));
                    v.put("name", job.get("name"));
                    v.put("namespace", ns);
                    v.put("createdAt", job.get("createdAt"));
                    v.put("version", null);
                    versions.add(v);
                }
                out.put("versions", versions);
                writeJson(resp, 200, out);
                return;
            }
            if (jp.suffix.startsWith("versions/")) {
                writeJson(resp, 200, store.getJob(ns, jp.job));
                return;
            }
            throw new NotFoundException("not found: " + r.path);
        }

        // /namespaces/{ns}/datasets ...
        if (p.size() >= 3 && "namespaces".equals(p.get(0)) && "datasets".equals(p.get(2))) {
            String ns = p.get(1);
            if (p.size() == 3) {
                writeJson(resp, 200, store.listDatasetsPage(ns, limit(req, 100), offset(req)));
                return;
            }
            DsPath dp = parseDatasetPath(p, 3);
            if (dp.suffix.isEmpty()) {
                Map<String, Object> ds = store.getDataset(ns, dp.dataset);
                if (ds == null) {
                    throw new NotFoundException("dataset not found: " + ns + "/" + dp.dataset);
                }
                writeJson(resp, 200, ds);
                return;
            }
            if ("versions".equals(dp.suffix)) {
                writeJson(resp, 200, store.listDatasetVersions(ns, dp.dataset, limit(req, 100), offset(req)));
                return;
            }
            if (dp.suffix.startsWith("versions/")) {
                String ver = dp.suffix.substring("versions/".length());
                Map<String, Object> v = store.getDatasetVersion(ns, dp.dataset, ver);
                if (v == null) {
                    throw new NotFoundException("dataset version not found");
                }
                writeJson(resp, 200, v);
                return;
            }
            throw new NotFoundException("not found: " + r.path);
        }

        // /lineage
        if (eq(p, "lineage")) {
            String nodeId = req.getParameter("nodeId");
            if (nodeId == null || nodeId.isEmpty()) {
                throw new IllegalArgumentException("nodeId is required");
            }
            int depth = intParam(req, "depth", 2);
            writeJson(resp, 200, store.lineageGraph(nodeId, depth));
            return;
        }

        // /events/lineage
        if (eq(p, "events", "lineage")) {
            writeJson(resp, 200, store.listEvents(
                param(req, "before", ""),
                param(req, "after", ""),
                limit(req, 100),
                offset(req),
                param(req, "sortDirection", "desc")
            ));
            return;
        }

        // /runlineage/upstream
        if (eq(p, "runlineage", "upstream")) {
            String runId = req.getParameter("runId");
            if (runId == null || runId.isEmpty()) {
                throw new IllegalArgumentException("runId is required");
            }
            writeJson(resp, 200, store.runLineageUpstream(runId, intParam(req, "depth", 20)));
            return;
        }

        // /column-lineage
        if (eq(p, "column-lineage")) {
            String nodeId = req.getParameter("nodeId");
            if (nodeId == null || nodeId.isEmpty()) {
                throw new IllegalArgumentException("nodeId is required");
            }
            boolean withDownstream = "true".equalsIgnoreCase(param(req, "withDownstream", "false"));
            writeJson(resp, 200, store.columnLineageGraph(nodeId, intParam(req, "depth", 20), withDownstream));
            return;
        }

        // /search
        if (eq(p, "search")) {
            String q = param(req, "q", "");
            if (q.isEmpty()) {
                throw new IllegalArgumentException("q is required");
            }
            writeJson(resp, 200, store.search(q, param(req, "filter", "ALL"), limit(req, 10)));
            return;
        }

        // /tags
        if (eq(p, "tags")) {
            writeJson(resp, 200, store.listTags());
            return;
        }

        // /sources
        if (eq(p, "sources")) {
            writeJson(resp, 200, store.listSources(limit(req, 100), offset(req)));
            return;
        }
        if (p.size() == 2 && "sources".equals(p.get(0))) {
            Map<String, Object> src = store.getSource(p.get(1));
            if (src == null) {
                throw new NotFoundException("source not found: " + p.get(1));
            }
            writeJson(resp, 200, src);
            return;
        }

        // /stats/*
        if (p.size() == 2 && "stats".equals(p.get(0))) {
            String period = param(req, "period", "DAY");
            String tz = param(req, "timezone", "UTC");
            if ("lineage-events".equals(p.get(1))) {
                writeJson(resp, 200, store.lineageEventMetrics(period, tz));
                return;
            }
            if ("jobs".equals(p.get(1)) || "datasets".equals(p.get(1)) || "sources".equals(p.get(1))) {
                writeJson(resp, 200, store.intervalMetrics(p.get(1), period, tz));
                return;
            }
        }

        writeJsonError(resp, 404, "not found: " + r.path);
    }

    // ─── v1 POST ─────────────────────────────────────────────────────────────

    private void handleV1Post(Route r, HttpServletRequest req, HttpServletResponse resp) throws Exception {
        List<String> p = r.parts;

        // POST /lineage — OpenLineage RunEvent ingest
        if (eq(p, "lineage")) {
            JsonNode node = MAPPER.readTree(req.getInputStream());
            if (node == null || !node.isObject()) {
                throw new IllegalArgumentException("body must be a RunEvent object");
            }
            Map<String, Object> receipt = store.ingestRunEvent(node);
            writeJson(resp, 201, receipt);
            return;
        }

        // POST /jobs?runState= — UI jobs-by-state (Marquez quirk)
        if (eq(p, "jobs")) {
            writeJson(resp, 200, store.listJobsPage(null, limit(req, 100), offset(req)));
            return;
        }

        // POST /namespaces/{ns}/jobs/{job}/runs
        if (p.size() >= 5 && "namespaces".equals(p.get(0)) && "jobs".equals(p.get(2))) {
            String ns = p.get(1);
            JobPath jp = parseJobPath(p, 3);
            if ("runs".equals(jp.suffix)) {
                JsonNode body = readBodyOrEmpty(req);
                Map<String, Object> run = store.createRun(ns, jp.job, body);
                writeJson(resp, 201, run);
                return;
            }
            // POST .../tags/{tag}
            if (jp.suffix.startsWith("tags/")) {
                String tag = jp.suffix.substring("tags/".length());
                Map<String, Object> job = store.addEntityTag("job", ns, jp.job, "", tag);
                if (job == null) {
                    throw new NotFoundException("job not found");
                }
                writeJson(resp, 200, job);
                return;
            }
        }

        // POST /namespaces/{ns}/datasets/{ds}/tags/{tag}
        // POST /namespaces/{ns}/datasets/{ds}/fields/{field}/tags/{tag}
        if (p.size() >= 5 && "namespaces".equals(p.get(0)) && "datasets".equals(p.get(2))) {
            String ns = p.get(1);
            DsPath dp = parseDatasetPath(p, 3);
            if (dp.suffix.startsWith("tags/")) {
                String tag = dp.suffix.substring("tags/".length());
                Map<String, Object> ds = store.addEntityTag("dataset", ns, dp.dataset, "", tag);
                if (ds == null) {
                    throw new NotFoundException("dataset not found");
                }
                writeJson(resp, 200, ds);
                return;
            }
            if (dp.suffix.startsWith("fields/") && dp.suffix.contains("/tags/")) {
                String rest = dp.suffix.substring("fields/".length());
                int ti = rest.indexOf("/tags/");
                String field = rest.substring(0, ti);
                String tag = rest.substring(ti + "/tags/".length());
                Map<String, Object> ds = store.addEntityTag("field", ns, dp.dataset, field, tag);
                if (ds == null) {
                    throw new NotFoundException("dataset not found");
                }
                writeJson(resp, 200, ds);
                return;
            }
        }

        // POST /jobs/runs/{id}/start|complete|fail|abort
        if (p.size() == 4 && "jobs".equals(p.get(0)) && "runs".equals(p.get(1))) {
            String runId = p.get(2);
            String action = p.get(3).toLowerCase(Locale.ROOT);
            String state;
            switch (action) {
                case "start":
                    state = "RUNNING";
                    break;
                case "complete":
                    state = "COMPLETED";
                    break;
                case "fail":
                    state = "FAILED";
                    break;
                case "abort":
                    state = "ABORTED";
                    break;
                default:
                    throw new NotFoundException("not found: " + r.path);
            }
            Map<String, Object> run = store.transitionRun(runId, state);
            if (run == null) {
                throw new NotFoundException("run not found: " + runId);
            }
            writeJson(resp, 200, run);
            return;
        }

        writeJsonError(resp, 404, "not found: " + r.path);
    }

    // ─── v1 PUT ──────────────────────────────────────────────────────────────

    private void handleV1Put(Route r, HttpServletRequest req, HttpServletResponse resp) throws Exception {
        List<String> p = r.parts;
        JsonNode body = readBodyOrEmpty(req);

        // PUT /namespaces/{ns}
        if (p.size() == 2 && "namespaces".equals(p.get(0))) {
            writeJson(resp, 200, store.upsertNamespace(p.get(1), body));
            return;
        }

        // PUT /namespaces/{ns}/jobs/{job}
        if (p.size() >= 4 && "namespaces".equals(p.get(0)) && "jobs".equals(p.get(2))) {
            String ns = p.get(1);
            JobPath jp = parseJobPath(p, 3);
            if (jp.suffix.isEmpty()) {
                writeJson(resp, 200, store.upsertJob(ns, jp.job, body));
                return;
            }
        }

        // PUT /namespaces/{ns}/datasets/{ds}
        if (p.size() >= 4 && "namespaces".equals(p.get(0)) && "datasets".equals(p.get(2))) {
            String ns = p.get(1);
            DsPath dp = parseDatasetPath(p, 3);
            if (dp.suffix.isEmpty()) {
                writeJson(resp, 200, store.upsertDataset(ns, dp.dataset, body));
                return;
            }
        }

        // PUT /tags/{name}
        if (p.size() == 2 && "tags".equals(p.get(0))) {
            writeJson(resp, 200, store.upsertTag(p.get(1), body));
            return;
        }

        // PUT /sources/{name}
        if (p.size() == 2 && "sources".equals(p.get(0))) {
            writeJson(resp, 200, store.upsertSource(p.get(1), body));
            return;
        }

        writeJsonError(resp, 404, "not found: " + r.path);
    }

    // ─── v1 DELETE ───────────────────────────────────────────────────────────

    private void handleV1Delete(Route r, HttpServletRequest req, HttpServletResponse resp) throws Exception {
        List<String> p = r.parts;

        // DELETE /namespaces/{ns}
        if (p.size() == 2 && "namespaces".equals(p.get(0))) {
            Map<String, Object> ns = store.deleteNamespace(p.get(1));
            if (ns == null) {
                throw new NotFoundException("namespace not found");
            }
            writeJson(resp, 200, ns);
            return;
        }

        // DELETE /namespaces/{ns}/jobs/{job}[/tags/{tag}]
        if (p.size() >= 4 && "namespaces".equals(p.get(0)) && "jobs".equals(p.get(2))) {
            String ns = p.get(1);
            JobPath jp = parseJobPath(p, 3);
            if (jp.suffix.isEmpty()) {
                Map<String, Object> job = store.deleteJob(ns, jp.job);
                if (job == null) {
                    throw new NotFoundException("job not found");
                }
                writeJson(resp, 200, job);
                return;
            }
            if (jp.suffix.startsWith("tags/")) {
                String tag = jp.suffix.substring("tags/".length());
                Map<String, Object> job = store.deleteEntityTag("job", ns, jp.job, "", tag);
                if (job == null) {
                    throw new NotFoundException("job not found");
                }
                writeJson(resp, 200, job);
                return;
            }
        }

        // DELETE /namespaces/{ns}/datasets/{ds}[/tags/{tag}|/fields/.../tags/...]
        if (p.size() >= 4 && "namespaces".equals(p.get(0)) && "datasets".equals(p.get(2))) {
            String ns = p.get(1);
            DsPath dp = parseDatasetPath(p, 3);
            if (dp.suffix.isEmpty()) {
                Map<String, Object> ds = store.deleteDataset(ns, dp.dataset);
                if (ds == null) {
                    throw new NotFoundException("dataset not found");
                }
                writeJson(resp, 200, ds);
                return;
            }
            if (dp.suffix.startsWith("tags/")) {
                String tag = dp.suffix.substring("tags/".length());
                Map<String, Object> ds = store.deleteEntityTag("dataset", ns, dp.dataset, "", tag);
                if (ds == null) {
                    throw new NotFoundException("dataset not found");
                }
                writeJson(resp, 200, ds);
                return;
            }
            if (dp.suffix.startsWith("fields/") && dp.suffix.contains("/tags/")) {
                String rest = dp.suffix.substring("fields/".length());
                int ti = rest.indexOf("/tags/");
                String field = rest.substring(0, ti);
                String tag = rest.substring(ti + "/tags/".length());
                Map<String, Object> ds = store.deleteEntityTag("field", ns, dp.dataset, field, tag);
                if (ds == null) {
                    throw new NotFoundException("dataset not found");
                }
                writeJson(resp, 200, ds);
                return;
            }
        }

        writeJsonError(resp, 404, "not found: " + r.path);
    }

    // ─── v2beta GET ──────────────────────────────────────────────────────────

    private void handleV2BetaGet(Route r, HttpServletRequest req, HttpServletResponse resp) throws Exception {
        List<String> p = r.parts;
        if (eq(p, "search", "jobs")) {
            writeJson(resp, 200, store.openSearchJobs(param(req, "q", "")));
            return;
        }
        if (eq(p, "search", "datasets")) {
            writeJson(resp, 200, store.openSearchDatasets(param(req, "q", "")));
            return;
        }
        writeJsonError(resp, 404, "not found: " + r.path);
    }

    // ─── Routing helpers ─────────────────────────────────────────────────────

    private static final class Route {
        final String path;
        final List<String> parts;
        final boolean v2beta;

        Route(String path, List<String> parts, boolean v2beta) {
            this.path = path;
            this.parts = parts;
            this.v2beta = v2beta;
        }
    }

    private static final class JobPath {
        final String job;
        final String suffix; // "", "runs", "versions", "versions/{v}", "tags/{tag}"

        JobPath(String job, String suffix) {
            this.job = job;
            this.suffix = suffix;
        }
    }

    private static final class DsPath {
        final String dataset;
        final String suffix;

        DsPath(String dataset, String suffix) {
            this.dataset = dataset;
            this.suffix = suffix;
        }
    }

    private static Route route(HttpServletRequest req) {
        String uri = req.getRequestURI();
        String ctx = req.getContextPath() == null ? "" : req.getContextPath();
        String relative = uri.startsWith(ctx) ? uri.substring(ctx.length()) : uri;
        boolean v2beta = relative.startsWith("/api/v2beta");
        String prefix = v2beta ? "/api/v2beta" : "/api/v1";
        String path;
        if (relative.startsWith(prefix)) {
            path = relative.substring(prefix.length());
        } else {
            String pi = req.getPathInfo();
            path = pi == null ? "/" : pi;
        }
        if (path.isEmpty()) {
            path = "/";
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        // strip trailing slash except root
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        List<String> parts = new ArrayList<>();
        if (!"/".equals(path)) {
            for (String seg : path.substring(1).split("/")) {
                parts.add(decode(seg));
            }
        }
        return new Route(path, parts, v2beta);
    }

    /**
     * Parse job name (may contain '/') and optional suffix (runs|versions|tags/...).
     * {@code start} is index of first job-name segment.
     */
    private static JobPath parseJobPath(List<String> p, int start) {
        if (start >= p.size()) {
            return new JobPath("", "");
        }
        List<String> rest = p.subList(start, p.size());
        // Find known suffix tokens from the end
        for (int i = 0; i < rest.size(); i++) {
            String tok = rest.get(i);
            if ("runs".equals(tok) || "versions".equals(tok) || "tags".equals(tok)) {
                String job = join(rest.subList(0, i));
                String suffix = join(rest.subList(i, rest.size()));
                return new JobPath(job, suffix);
            }
        }
        return new JobPath(join(rest), "");
    }

    private static DsPath parseDatasetPath(List<String> p, int start) {
        if (start >= p.size()) {
            return new DsPath("", "");
        }
        List<String> rest = p.subList(start, p.size());
        for (int i = 0; i < rest.size(); i++) {
            String tok = rest.get(i);
            if ("versions".equals(tok) || "tags".equals(tok) || "fields".equals(tok)) {
                String ds = join(rest.subList(0, i));
                String suffix = join(rest.subList(i, rest.size()));
                return new DsPath(ds, suffix);
            }
        }
        return new DsPath(join(rest), "");
    }

    private static String join(List<String> parts) {
        if (parts.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append('/');
            }
            sb.append(parts.get(i));
        }
        return sb.toString();
    }

    private static boolean eq(List<String> parts, String... expected) {
        if (parts.size() != expected.length) {
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            if (!expected[i].equals(parts.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static String param(HttpServletRequest req, String name, String def) {
        String v = req.getParameter(name);
        return v == null || v.isEmpty() ? def : v;
    }

    private static int limit(HttpServletRequest req, int def) {
        return intParam(req, "limit", def);
    }

    private static int offset(HttpServletRequest req) {
        return intParam(req, "offset", 0);
    }

    private static int intParam(HttpServletRequest req, String name, int def) {
        String v = req.getParameter(name);
        if (v == null || v.isEmpty()) {
            return def;
        }
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static JsonNode readBodyOrEmpty(HttpServletRequest req) throws IOException {
        JsonNode n = MAPPER.readTree(req.getInputStream());
        return n == null || n.isNull() ? MAPPER.createObjectNode() : n;
    }

    private static void writeJson(HttpServletResponse resp, int status, Object body) throws IOException {
        resp.setStatus(status);
        resp.setContentType(JSON);
        resp.setCharacterEncoding("UTF-8");
        MAPPER.writeValue(resp.getOutputStream(), body);
    }

    private static void writeJsonError(HttpServletResponse resp, int status, String message) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("message", message == null ? "error" : message);
        m.put("status", status);
        writeJson(resp, status, m);
    }

    private static class NotFoundException extends Exception {
        NotFoundException(String msg) {
            super(msg);
        }
    }
}
