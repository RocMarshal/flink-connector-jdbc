/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.flink.connector.jdbc.source.Utils.deserializeJdbcSourceSplit;
import static org.apache.flink.connector.jdbc.source.Utils.serializeJdbcSourceSplit;
import static org.apache.flink.util.Preconditions.checkArgument;

/** The class is used to de/serialize the {@link JdbcSourceSplit}. */
@Internal
public class JdbcSourceSplitSerializer implements SimpleVersionedSerializer<JdbcSourceSplit> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JdbcSourceSplit split) throws IOException {

        checkArgument(
                split.getClass() == JdbcSourceSplit.class,
                "Cannot serialize classes of JdbcSourceSplit");

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializeJdbcSourceSplit(out, split);

            out.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JdbcSourceSplit deserialize(int version, byte[] serialized) throws IOException {

        if (version == CURRENT_VERSION) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private JdbcSourceSplit deserializeV1(byte[] serialized) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            return deserializeJdbcSourceSplit(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}