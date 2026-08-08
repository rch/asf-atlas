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
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * OpenLineage HTTP ingest + Marquez-compatible read API.
 * Served at {@code /api/v1/*} when jersey-servlet is mapped there (see web.xml).
 * Does not alter {@code /api/atlas/v2/*} governance clients.
 */
@Path("")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class OpenLineageREST {
    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageREST.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OpenLineageStore store;

    @Inject
    public OpenLineageREST(OpenLineageStore store) {
        this.store = store;
    }

    @GET
    @Path("health")
    public Map<String, Object> health() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", store.isReady() ? "ok" : "degraded");
        m.put("surface", "openlineage-marquez-v1");
        return m;
    }

    /** OpenLineage RunEvent ingest (HttpTransport / Flink OL backend). */
    @POST
    @Path("lineage")
    public Response postLineage(String body) {
        try {
            JsonNode node = MAPPER.readTree(body);
            if (!node.isObject()) {
                throw new WebApplicationException("body must be a RunEvent object", Response.Status.BAD_REQUEST);
            }
            Map<String, Object> receipt = store.ingestRunEvent(node);
            return Response.status(Response.Status.CREATED).entity(receipt).build();
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.BAD_REQUEST);
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("OpenLineage ingest failed", e);
            throw new WebApplicationException("ingest failed: " + e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("namespaces")
    public Map<String, Object> namespaces() {
        try {
            List<String> ns = store.listNamespaces();
            List<Map<String, Object>> list = new ArrayList<>();
            for (String n : ns) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", n);
                m.put("isHidden", false);
                list.add(m);
            }
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("namespaces", list);
            return resp;
        } catch (Exception e) {
            LOG.error("list namespaces failed", e);
            throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("namespaces/{ns}/jobs")
    public Map<String, Object> jobs(@PathParam("ns") String ns) {
        try {
            List<Map<String, Object>> jobs = store.listJobs(ns);
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("jobs", jobs);
            resp.put("totalCount", jobs.size());
            return resp;
        } catch (Exception e) {
            LOG.error("list jobs failed", e);
            throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("namespaces/{ns}/jobs/{job}/runs")
    public Map<String, Object> runs(@PathParam("ns") String ns, @PathParam("job") String job) {
        try {
            List<Map<String, Object>> runs = store.listRuns(ns, job);
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("runs", runs);
            resp.put("totalCount", runs.size());
            return resp;
        } catch (Exception e) {
            LOG.error("list runs failed", e);
            throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("namespaces/{ns}/datasets")
    public Map<String, Object> datasets(@PathParam("ns") String ns) {
        try {
            List<Map<String, Object>> ds = store.listDatasets(ns);
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("datasets", ds);
            resp.put("totalCount", ds.size());
            return resp;
        } catch (Exception e) {
            LOG.error("list datasets failed", e);
            throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    /** Marquez-web core: bipartite job/dataset graph around nodeId. */
    @GET
    @Path("lineage")
    public Map<String, Object> lineage(
            @QueryParam("nodeId") String nodeId,
            @QueryParam("depth") @DefaultValue("2") int depth) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new WebApplicationException("nodeId is required", Response.Status.BAD_REQUEST);
        }
        try {
            return store.lineageGraph(nodeId, depth);
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.BAD_REQUEST);
        } catch (Exception e) {
            LOG.error("lineage graph failed", e);
            throw new WebApplicationException(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
