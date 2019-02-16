/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package us.weeksconsulting.processors.cdf;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class TestConvertCDFToAVRO {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConvertCDFToAVRO.class);
    }

    @Test
    public void testProcessor() throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get("E:\\LOGSA\\cdf_sample2"))) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                if (path.getFileName().toString().toLowerCase().endsWith(".cdf")) {
//                    System.out.println("Processing " + path.getFileName());
                    try {
                        testRunner.enqueue(path);
                        testRunner.run();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        for (MockFlowFile flowFile : testRunner.getFlowFilesForRelationship(ConvertCDFToAVRO.REL_SUCCESS)) {
            System.out.println("-- Starting Dump of " + flowFile.getAttribute("filename") + " " + flowFile.getSize() / 1024.00 / 1024.00 + " MBytes --");

            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(flowFile.toByteArray()), datumReader);
            System.out.println(dataFileReader.getSchema());
            System.out.println(new String(dataFileReader.getMeta("avro.codec")));
            for (GenericRecord r : dataFileReader) {
                System.out.println(r.toString());
            }

            System.out.println("-- Finished Dump --");
        }
    }

}
