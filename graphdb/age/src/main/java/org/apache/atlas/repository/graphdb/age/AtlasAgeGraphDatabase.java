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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtlasAgeGraphDatabase implements GraphDatabase<AtlasAgeVertex, AtlasAgeEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAgeGraphDatabase.class);

    private static final String CONF_JDBC_URL    = "atlas.age.jdbc.url";
    private static final String CONF_JDBC_USER   = "atlas.age.jdbc.user";
    private static final String CONF_JDBC_PASS   = "atlas.age.jdbc.password";
    private static final String CONF_GRAPH_NAME  = "atlas.age.graph.name";
    private static final String CONF_POOL_SIZE   = "atlas.age.pool.size";

    private static final String DEFAULT_JDBC_URL   = "jdbc:postgresql://localhost:5432/signals";
    private static final String DEFAULT_JDBC_USER  = "signals";
    private static final String DEFAULT_GRAPH_NAME = "atlas_graph";
    private static final int    DEFAULT_POOL_SIZE  = 10;

    private static volatile AtlasAgeGraph  graphInstance;
    private static volatile AgeConnectionPool connectionPool;

    public AtlasAgeGraphDatabase() {
        LOG.info("AtlasAgeGraphDatabase instantiated");
    }

    @Override
    public boolean isGraphLoaded() {
        return graphInstance != null;
    }

    @Override
    public AtlasGraph<AtlasAgeVertex, AtlasAgeEdge> getGraph() {
        AtlasAgeGraph graph = graphInstance;

        if (graph == null) {
            synchronized (AtlasAgeGraphDatabase.class) {
                graph = graphInstance;

                if (graph == null) {
                    graph = initGraph();
                    graphInstance = graph;
                }
            }
        }

        return graph;
    }

    @Override
    public AtlasGraph<AtlasAgeVertex, AtlasAgeEdge> getGraphBulkLoading() {
        return getGraph();
    }

    @Override
    public void initializeTestGraph() {
        getGraph();
    }

    @Override
    public void cleanup() {
        synchronized (AtlasAgeGraphDatabase.class) {
            if (graphInstance != null) {
                graphInstance.clear();
            }
        }
    }

    private static AtlasAgeGraph initGraph() {
        try {
            Configuration config = ApplicationProperties.get();

            String jdbcUrl   = config.getString(CONF_JDBC_URL, DEFAULT_JDBC_URL);
            String jdbcUser  = config.getString(CONF_JDBC_USER, DEFAULT_JDBC_USER);
            String jdbcPass  = config.getString(CONF_JDBC_PASS, "");
            String graphName = config.getString(CONF_GRAPH_NAME, DEFAULT_GRAPH_NAME);
            int    poolSize  = config.getInt(CONF_POOL_SIZE, DEFAULT_POOL_SIZE);

            LOG.info("Initializing AGE graph: url={}, user={}, graph={}, poolSize={}",
                    jdbcUrl, jdbcUser, graphName, poolSize);

            connectionPool = new AgeConnectionPool(jdbcUrl, jdbcUser, jdbcPass, poolSize);

            return new AtlasAgeGraph(connectionPool, graphName);
        } catch (AtlasException e) {
            throw new RuntimeException("Failed to initialize AGE graph database", e);
        }
    }
}
