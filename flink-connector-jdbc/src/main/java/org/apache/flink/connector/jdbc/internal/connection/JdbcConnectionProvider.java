/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

/** JDBC connection provider. */
@PublicEvolving
public interface JdbcConnectionProvider {
    /**
     * Get existing connection.
     *
     * @return existing connection
     */
    @Nullable
    Connection getConnection();

    /**
     * Check whether possible existing connection is valid or not through {@link
     * Connection#isValid(int)}.
     *
     * @return true if existing connection is valid
     * @throws SQLException sql exception throw from {@link Connection#isValid(int)}
     */
    boolean isConnectionValid() throws SQLException;

    /**
     * Get existing connection or establish an new one if there is none.
     *
     * @return existing connection or newly established connection
     * @throws SQLException sql exception
     * @throws ClassNotFoundException driver class not found
     */
    Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException;

    /** Close possible existing connection. */
    void closeConnection();

    /**
     * Close possible existing connection and establish an new one.
     *
     * @return newly established connection
     * @throws SQLException sql exception
     * @throws ClassNotFoundException driver class not found
     */
    Connection reestablishConnection() throws SQLException, ClassNotFoundException;
}
