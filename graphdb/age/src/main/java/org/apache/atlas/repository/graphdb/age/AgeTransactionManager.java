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
import java.sql.SQLException;

public class AgeTransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(AgeTransactionManager.class);

    private final AgeConnectionPool connectionPool;
    private final ThreadLocal<Connection> threadConnection = new ThreadLocal<>();

    public AgeTransactionManager(AgeConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    public Connection getConnection() throws SQLException {
        Connection conn = threadConnection.get();

        if (conn == null || conn.isClosed()) {
            conn = connectionPool.getConnection();
            conn.setAutoCommit(false);
            threadConnection.set(conn);
        }

        return conn;
    }

    public boolean hasActiveConnection() {
        Connection conn = threadConnection.get();
        try {
            return conn != null && !conn.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    public void commit() {
        Connection conn = threadConnection.get();

        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                LOG.error("Failed to commit transaction", e);
                throw new RuntimeException("Failed to commit transaction", e);
            }
        }
    }

    public void rollback() {
        Connection conn = threadConnection.get();

        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                LOG.error("Failed to rollback transaction", e);
            }
        }
    }

    public void releaseConnection() {
        Connection conn = threadConnection.get();

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Failed to close connection", e);
            } finally {
                threadConnection.remove();
            }
        }
    }

    public void commitAndRelease() {
        try {
            commit();
        } finally {
            releaseConnection();
        }
    }

    public void rollbackAndRelease() {
        try {
            rollback();
        } finally {
            releaseConnection();
        }
    }
}
