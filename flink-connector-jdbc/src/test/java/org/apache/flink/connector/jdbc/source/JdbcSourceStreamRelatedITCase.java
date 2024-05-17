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

package org.apache.flink.connector.jdbc.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.databases.mysql.MySqlTestBase;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.split.JdbcSlideTimingParameterProvider;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.connector.jdbc.utils.ContinuousEnumerationSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for streaming semantic related cases of {@link JdbcSource}. */
class JdbcSourceStreamRelatedITCase implements MySqlTestBase, JdbcITCaseBase {

    private static final long ONE_SECOND = Duration.ofSeconds(1L).toMillis();
    private static final int TESTING_PARALLELISM = 2;
    private static final long INTERVAL_OF_GENERATING = 50L;
    private static final int TESTING_ENTRIES_SIZE = 200;
    private static final int DATA_NUM_PER_SECOND_SPAN_SPLIT =
            (int) (TESTING_ENTRIES_SIZE * INTERVAL_OF_GENERATING / TESTING_ENTRIES_SIZE);
    private static final String testingTable = "t_testing";
    private static final String CREATE_SQL =
            "CREATE TABLE if not exists "
                    + testingTable
                    + " ("
                    + "id bigint NOT NULL, "
                    + "ts bigint NOT NULL, "
                    + "PRIMARY KEY (id))";
    private static final ContinuousEnumerationSettings CONTINUOUS_SETTINGS =
            new ContinuousEnumerationSettings(Duration.ofSeconds(1));
    private static final ResultExtractor<TestEntry> EXTRACTOR =
            resultSet -> new TestEntry(resultSet.getLong("id"), resultSet.getLong("ts"));
    private static final List<TestEntry> testEntries = new ArrayList<>(TESTING_ENTRIES_SIZE);

    private static Queue<TestEntry> collectedRecords;
    private static long globalStartMillis;
    private static long globalDataEndMillis;

    private JdbcSourceBuilder<TestEntry> jdbcSourceBuilder;

    @BeforeEach
    void initData() {
        testEntries.clear();
        quickExecutionSQL(CREATE_SQL);
        quickExecutionSQL("delete from " + testingTable);
        generateTestEntries();
        String insertSQL = generateInsertSQL();
        quickExecutionSQL(insertSQL);

        JdbcSlideTimingParameterProvider slideTimingParamsProvider =
                new JdbcSlideTimingParameterProvider(
                        globalStartMillis, ONE_SECOND, ONE_SECOND, 100L);
        jdbcSourceBuilder =
                JdbcSource.<TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(TestEntry.class))
                        .setSql("select * from " + testingTable + " where ts >= ? and ts < ?")
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setUsername(getMetadata().getUsername())
                        .setPassword(getMetadata().getPassword())
                        .setContinuousEnumerationSettings(CONTINUOUS_SETTINGS)
                        .setJdbcParameterValuesProvider(slideTimingParamsProvider)
                        .setDriverName(getMetadata().getDriverClass())
                        .setResultExtractor(EXTRACTOR);

        collectedRecords = new ConcurrentLinkedDeque<>();
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void testForNormalCaseWithoutFailure(
            DeliveryGuarantee guarantee, @InjectClusterClient ClusterClient<?> client)
            throws Exception {
        // Test continuous + unbounded splits
        StreamExecutionEnvironment env = getEnvWithRestartStrategyParallelism();

        jdbcSourceBuilder.setDeliveryGuarantee(guarantee);
        if (DeliveryGuarantee.EXACTLY_ONCE == guarantee) {
            jdbcSourceBuilder.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        }
        env.fromSource(jdbcSourceBuilder.build(), WatermarkStrategy.noWatermarks(), "TestSource")
                .addSink(new TestingSinkFunction());
        waitExpectation(client, env, () -> collectedRecords.size() >= TESTING_ENTRIES_SIZE, 100L);

        assertThat(collectedRecords).containsExactlyInAnyOrderElementsOf(testEntries);
    }

    @Test
    void testExactlyOnceWithFailure(@InjectClusterClient ClusterClient<?> client) throws Exception {
        // Test continuous + unbounded splits
        StreamExecutionEnvironment env = getEnvWithRestartStrategyParallelism();
        jdbcSourceBuilder
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE);

        env.fromSource(jdbcSourceBuilder.build(), WatermarkStrategy.noWatermarks(), "TestSource")
                .keyBy(testEntry -> 0L)
                .process(new TestingKeyProcessFunction())
                .setParallelism(1);

        waitExpectation(client, env, () -> collectedRecords.size() >= TESTING_ENTRIES_SIZE, 1000L);

        assertThat(collectedRecords).containsExactlyInAnyOrderElementsOf(testEntries);
    }

    @Test
    void testAtLeastOnceWithFailure(@InjectClusterClient ClusterClient<?> client) throws Exception {
        // Test continuous + unbounded splits
        StreamExecutionEnvironment env = getEnvWithRestartStrategyParallelism();
        jdbcSourceBuilder.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);

        env.fromSource(jdbcSourceBuilder.build(), WatermarkStrategy.noWatermarks(), "TestSource")
                .keyBy(testEntry -> 0L)
                .process(new TestingKeyProcessFunction())
                .setParallelism(1);

        waitExpectation(
                client,
                env,
                () -> new HashSet<>(collectedRecords).size() >= TESTING_ENTRIES_SIZE,
                500L);

        assertThat(collectedRecords)
                .hasSizeGreaterThanOrEqualTo(testEntries.size())
                .containsAll(testEntries);
    }

    @Test
    void testAtMostOnceWithFailure(@InjectClusterClient ClusterClient<?> client) throws Exception {
        // Test continuous + unbounded splits
        StreamExecutionEnvironment env = getEnvWithRestartStrategyParallelism();

        env.fromSource(jdbcSourceBuilder.build(), WatermarkStrategy.noWatermarks(), "TestSource")
                .keyBy(testEntry -> 0L)
                .process(new TestingKeyProcessFunction())
                .setParallelism(1);

        waitExpectation(
                client,
                env,
                () ->
                        Math.abs(collectedRecords.size() - testEntries.size())
                                <= DATA_NUM_PER_SECOND_SPAN_SPLIT,
                50L);

        assertThat(testEntries)
                .hasSizeGreaterThanOrEqualTo(collectedRecords.size())
                .containsAll(collectedRecords);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitExpectation(
            ClusterClient<?> client,
            StreamExecutionEnvironment env,
            Supplier<Boolean> condition,
            long additionalWatMillis)
            throws Exception {
        JobID jobID = env.executeAsync().getJobID();
        CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> {
                            while (true) {
                                if (condition.get()) {
                                    sleep(additionalWatMillis);
                                    client.cancel(jobID);
                                    break;
                                }
                            }
                        });
        future.get();
    }

    private void generateTestEntries() {
        // The data is distributed in 1 min.
        long millisAnchor = System.currentTimeMillis();
        globalStartMillis = millisAnchor - INTERVAL_OF_GENERATING * TESTING_ENTRIES_SIZE / 2;
        globalDataEndMillis =
                globalDataEndMillis + millisAnchor + INTERVAL_OF_GENERATING * TESTING_ENTRIES_SIZE;
        long startMillis = globalStartMillis;
        for (int i = 0; i < TESTING_ENTRIES_SIZE; i++) {
            testEntries.add(new TestEntry(i + 1, startMillis));
            startMillis += INTERVAL_OF_GENERATING;
        }
    }

    @Nonnull
    private static String generateInsertSQL() {
        StringBuilder sqlQueryBuilder =
                new StringBuilder("INSERT INTO " + testingTable + " (id, ts) VALUES ");
        for (int i = 0; i < testEntries.size(); i++) {
            sqlQueryBuilder
                    .append("(")
                    .append(testEntries.get(i).id)
                    .append(",")
                    .append(testEntries.get(i).ts)
                    .append(")");
            if (i < testEntries.size() - 1) {
                sqlQueryBuilder.append(",");
            }
        }
        return sqlQueryBuilder.toString();
    }

    private void quickExecutionSQL(String testingTable) {
        try (Connection conn = getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute(testingTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                getMetadata().getJdbcUrl(),
                getMetadata().getUsername(),
                getMetadata().getPassword());
    }

    @Nonnull
    private static StreamExecutionEnvironment getEnvWithRestartStrategyParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.FallbackRestartStrategyConfiguration());

        env.setParallelism(TESTING_PARALLELISM);
        env.enableCheckpointing(MINIMAL_CHECKPOINT_TIME);
        return env;
    }

    public static class TestEntry implements Serializable {
        public long id;
        public long ts;

        public TestEntry(long id, long ts) {
            this.id = id;
            this.ts = ts;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            TestEntry testEntry = (TestEntry) object;
            return id == testEntry.id && ts == testEntry.ts;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, ts);
        }

        @Override
        public String toString() {
            return "TestEntry{" + "id=" + id + ", ts=" + ts + '}';
        }
    }

    /** A process function for testing. */
    static class TestingKeyProcessFunction
            extends KeyedProcessFunction<Long, TestEntry, TestEntry> {
        private transient ListState<TestEntry> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState =
                    getRuntimeContext()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "collectedElements", TestEntry.class));
        }

        @Override
        public void processElement(
                TestEntry value,
                KeyedProcessFunction<Long, TestEntry, TestEntry>.Context ctx,
                Collector<TestEntry> out)
                throws Exception {
            if (value.id == testEntries.size() / 2 && getRuntimeContext().getAttemptNumber() < 1) {
                throw new RuntimeException();
            }
            listState.add(value);
            collectedRecords.clear();
            listState.get().forEach(collectedRecords::add);
            if (value.id % 17 == 0) {
                sleep(MINIMAL_CHECKPOINT_TIME * 2);
            }
        }
    }

    /** A sink function to collect the records. */
    static class TestingSinkFunction implements SinkFunction<TestEntry> {

        @Override
        public void invoke(TestEntry value, Context context) throws Exception {
            collectedRecords.add(value);
        }
    }
}
