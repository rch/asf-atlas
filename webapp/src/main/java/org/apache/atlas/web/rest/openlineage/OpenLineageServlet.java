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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OpenLineage ingest + Marquez-compat read surface at {@code /api/v1/*}.
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

    private static final Pattern JOBS = Pattern.compile("^/namespaces/([^/]+)/jobs/?$");
    private static final Pattern RUNS = Pattern.compile("^/namespaces/([^/]+)/jobs/([^/]+)/runs/?$");
    private static final Pattern DATASETS = Pattern.compile("^/namespaces/([^/]+)/datasets/?$");

    private OpenLineageStore store;

    @Override
    public void init() throws ServletException {
        WebApplicationContext ctx = WebApplicationContextUtils.getRequiredWebApplicationContext(getServletContext());
        store = ctx.getBean(OpenLineageStore.class);
        LOG.info("OpenLineageServlet ready (store ready={})", store.isReady());
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String path = pathInfo(req);
        try {
            if ("/health".equals(path) || path.isEmpty() || "/".equals(path)) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("status", store.isReady() ? "ok" : "degraded");
                m.put("surface", "openlineage-marquez-v1");
                writeJson(resp, 200, m);
                return;
            }
            if ("/namespaces".equals(path) || "/namespaces/".equals(path)) {
                List<String> ns = store.listNamespaces();
                List<Map<String, Object>> list = new ArrayList<>();
                for (String n : ns) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", n);
                    m.put("isHidden", false);
                    list.add(m);
                }
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("namespaces", list);
                writeJson(resp, 200, out);
                return;
            }
            Matcher mJobs = JOBS.matcher(path);
            if (mJobs.matches()) {
                String ns = decode(mJobs.group(1));
                List<Map<String, Object>> jobs = store.listJobs(ns);
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("jobs", jobs);
                out.put("totalCount", jobs.size());
                writeJson(resp, 200, out);
                return;
            }
            Matcher mRuns = RUNS.matcher(path);
            if (mRuns.matches()) {
                String ns = decode(mRuns.group(1));
                String job = decode(mRuns.group(2));
                List<Map<String, Object>> runs = store.listRuns(ns, job);
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("runs", runs);
                out.put("totalCount", runs.size());
                writeJson(resp, 200, out);
                return;
            }
            Matcher mDs = DATASETS.matcher(path);
            if (mDs.matches()) {
                String ns = decode(mDs.group(1));
                List<Map<String, Object>> ds = store.listDatasets(ns);
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("datasets", ds);
                out.put("totalCount", ds.size());
                writeJson(resp, 200, out);
                return;
            }
            if ("/lineage".equals(path)) {
                String nodeId = req.getParameter("nodeId");
                if (nodeId == null || nodeId.isEmpty()) {
                    writeText(resp, 400, "nodeId is required");
                    return;
                }
                int depth = 2;
                String depthStr = req.getParameter("depth");
                if (depthStr != null && !depthStr.isEmpty()) {
                    try {
                        depth = Integer.parseInt(depthStr);
                    } catch (NumberFormatException ignore) {
                        // keep default
                    }
                }
                writeJson(resp, 200, store.lineageGraph(nodeId, depth));
                return;
            }
            writeText(resp, 404, "not found: " + path);
        } catch (IllegalArgumentException e) {
            writeText(resp, 400, e.getMessage());
        } catch (Exception e) {
            LOG.error("OpenLineage GET {} failed", path, e);
            writeText(resp, 500, "error: " + e.getMessage());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String path = pathInfo(req);
        if (!"/lineage".equals(path)) {
            writeText(resp, 404, "not found: " + path);
            return;
        }
        try {
            JsonNode node = MAPPER.readTree(req.getInputStream());
            if (node == null || !node.isObject()) {
                writeText(resp, 400, "body must be a RunEvent object");
                return;
            }
            Map<String, Object> receipt = store.ingestRunEvent(node);
            writeJson(resp, 201, receipt);
        } catch (IllegalArgumentException e) {
            writeText(resp, 400, e.getMessage());
        } catch (Exception e) {
            LOG.error("OpenLineage ingest failed", e);
            writeText(resp, 500, "ingest failed: " + e.getMessage());
        }
    }

    private static String pathInfo(HttpServletRequest req) {
        String pi = req.getPathInfo();
        if (pi == null || pi.isEmpty()) {
            // url-pattern /api/v1/* may leave path in servletPath depending on container
            String uri = req.getRequestURI();
            String ctx = req.getContextPath() == null ? "" : req.getContextPath();
            String prefix = ctx + "/api/v1";
            if (uri.startsWith(prefix)) {
                pi = uri.substring(prefix.length());
            } else {
                pi = "/";
            }
        }
        if (pi.isEmpty()) {
            return "/";
        }
        return pi.startsWith("/") ? pi : "/" + pi;
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static void writeJson(HttpServletResponse resp, int status, Object body) throws IOException {
        resp.setStatus(status);
        resp.setContentType(JSON);
        resp.setCharacterEncoding("UTF-8");
        MAPPER.writeValue(resp.getOutputStream(), body);
    }

    private static void writeText(HttpServletResponse resp, int status, String body) throws IOException {
        resp.setStatus(status);
        resp.setContentType("text/plain; charset=UTF-8");
        resp.setCharacterEncoding("UTF-8");
        byte[] bytes = (body == null ? "" : body).getBytes(StandardCharsets.UTF_8);
        resp.getOutputStream().write(bytes);
    }
}
