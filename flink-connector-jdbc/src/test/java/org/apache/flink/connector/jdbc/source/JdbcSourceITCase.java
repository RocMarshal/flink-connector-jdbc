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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.source.enumerator.ContinuousEnumerationSettings;
import org.apache.flink.connector.jdbc.source.enumerator.SqlTemplateSplitEnumerator;
import org.apache.flink.connector.jdbc.source.reader.extractor.RowResultExtractor;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.ROW_TYPE_INFO;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS_SPLIT_BY_ID;
import static org.apache.flink.connector.jdbc.testutils.databases.db2.Db2Database.getMetadata;

class JdbcSourceITCase {

    @Test
    void test() throws Exception {
        JdbcSource<Row> jdbcSource =
                new JdbcSourceBuilder<Row>()
                        .setDBUrl("jdbc:mysql://localhost:3306/test")
                        .setDriverName("com.mysql.cj.jdbc.Driver")
                        .setResultExtractor(new RowResultExtractor())
                        .setPassword("123456789")
                        .setUsername("root")
                        .setTypeInformation(TypeInformation.of(new TypeHint<Row>() {}))
                        .setSqlSplitEnumeratorProvider(
                                new SqlTemplateSplitEnumerator.TemplateSqlSplitEnumeratorProvider()
                                        .setSqlTemplate("select * from t1 where id=?")
                                        .setParameterValuesProvider(
                                                new JdbcGenericParameterValuesProvider(
                                                        new Serializable[][] {{1}, {1}})))
                        .setContinuousEnumerationSettings(
                                new ContinuousEnumerationSettings(Duration.ofSeconds(5)))
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "mysource").print();
        env.execute();
    }

    @Test
    void testgggSet() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        JdbcInputFormat.JdbcInputFormatBuilder inputBuilder =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO);
        // use a "splittable" query to exploit parallelism
        inputBuilder =
                inputBuilder
                        .setQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID)
                        .setParametersProvider(
                                () ->
                                        new Serializable[][] {
                                            {1001, 1002}, {1004, 1005}, {1007, 1009}
                                        });
        environment.setParallelism(2);
        DataSet<Row> source = environment.createInput(inputBuilder.finish());

        source.collect();
    }

    @Test
    void testgggStream() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        JdbcInputFormat.JdbcInputFormatBuilder inputBuilder =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO);
        inputBuilder =
                inputBuilder
                        .setQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID)
                        .setParametersProvider(
                                () ->
                                        new Serializable[][] {
                                            {1001, 1002}, {1004, 1005}, {1007, 1009}
                                        });
        environment.setParallelism(2);
        environment.createInput(inputBuilder.finish()).addSink(new DiscardingSink<>());

        environment.execute();
    }
}
