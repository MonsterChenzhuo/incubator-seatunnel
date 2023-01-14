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

package org.apache.seatunnel.format.json.canal;

import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CanalJsonSerDeSchemaTest {

    private static final SeaTunnelRowType PHYSICAL_DATA_TYPE =
        new SeaTunnelRowType(new String[]{"id", "name", "description", "weight"}, new SeaTunnelDataType[]{INT_TYPE, STRING_TYPE, STRING_TYPE, STRING_TYPE});

    @Test
    public void testFilteringTables() throws Exception {
        List<String> lines = readLines("canal-data-filter-table.txt");
        CanalJsonDeserializationSchema deserializationSchema =
            new CanalJsonDeserializationSchema.Builder(
                PHYSICAL_DATA_TYPE)
                .setDatabase("^my.*")
                .setTable("^prod.*")
                .build();
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testDeserializeNullRow() throws Exception {
        final CanalJsonDeserializationSchema deserializationSchema =
            createCanalJsonDeserializationSchema(null, null);
        final SimpleCollector collector = new SimpleCollector();

        deserializationSchema.collect(null, collector);
        deserializationSchema.collect(new byte[0], collector);
        assertEquals(0, collector.list.size());
    }

    public void runTest(List<String> lines, CanalJsonDeserializationSchema deserializationSchema)
        throws Exception {
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.collect(line.getBytes(StandardCharsets.UTF_8), collector);
        }

        List<String> expected =
            Arrays.asList(
                "SeaTunnelRow{tableId=-1, kind=-U, fields=[101, scooter, Small 2-wheel scooter, 3.14]}",
                "SeaTunnelRow{tableId=-1, kind=+U, fields=[101, scooter, Small 2-wheel scooter, 5.17]}",
                "SeaTunnelRow{tableId=-1, kind=-U, fields=[102, car battery, 12V car battery, 8.1]}",
                "SeaTunnelRow{tableId=-1, kind=+U, fields=[102, car battery, 12V car battery, 5.17]}",
                "SeaTunnelRow{tableId=-1, kind=-D, fields=[103, car battery, 12V car battery, 5.17]}",
                "SeaTunnelRow{tableId=-1, kind=-D, fields=[104, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]}");
        List<String> actual =
            collector.list.stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(expected, actual);

        // test Serialization
        CanalJsonSerializationSchema serializationSchema =
            new CanalJsonSerializationSchema(
                PHYSICAL_DATA_TYPE);
        List<String> result = new ArrayList<>();
        for (SeaTunnelRow rowData : collector.list) {
            result.add(new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
        }

        List<String> expectedResult =
            Arrays.asList(
                "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"3.14\"},\"type\":\"DELETE\"}",
                "{\"data\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"5.17\"},\"type\":\"INSERT\"}",
                "{\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":\"8.1\"},\"type\":\"DELETE\"}",
                "{\"data\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":\"5.17\"},\"type\":\"INSERT\"}",
                "{\"data\":{\"id\":103,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":\"5.17\"},\"type\":\"DELETE\"}",
                "{\"data\":{\"id\":104,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":\"0.8\"},\"type\":\"DELETE\"}"
            );
        assertEquals(expectedResult, result);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private CanalJsonDeserializationSchema createCanalJsonDeserializationSchema(
        String database, String table) {
        return CanalJsonDeserializationSchema.builder(
                PHYSICAL_DATA_TYPE)
            .setDatabase(database)
            .setTable(table)
            .setIgnoreParseErrors(false)
            .build();
    }

    private static List<String> readLines(String resource) throws IOException {
        final URL url = CanalJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static class SimpleCollector implements Collector<SeaTunnelRow> {

        private List<SeaTunnelRow> list = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            list.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }

    }
}