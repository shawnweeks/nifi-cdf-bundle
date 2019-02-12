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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.List;


public class TestConvertCDFToJSON {

    private TestRunner testRunner;


    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConvertCDFToJSON.class);
    }

    @Test
    public void testProcessor() throws IOException {
//        testRunner.enqueue(Paths.get("src/test/resources/bigcdf_compressed.cdf"));
        testRunner.enqueue(Paths.get("src/test/resources/cl_sp_edi_00000000_v01.cdf"));
        testRunner.run();

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ConvertCDFToJSON.REL_SUCCESS).get(0);
        byte [] ba = out.toByteArray();
        System.out.println(ba.length);
        String output = new String(ba,"UTF-8");
        List<String> lines = IOUtils.readLines(new StringReader(output));
        for(String line: lines){
            System.out.println(line);
        }
    }

}
