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

import com.google.gson.stream.JsonWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.w3c.dom.Attr;
import uk.ac.bristol.star.cdf.AttributeEntry;
import uk.ac.bristol.star.cdf.CdfContent;
import uk.ac.bristol.star.cdf.CdfInfo;
import uk.ac.bristol.star.cdf.CdfReader;
import uk.ac.bristol.star.cdf.GlobalAttribute;
import uk.ac.bristol.star.cdf.Variable;
import uk.ac.bristol.star.cdf.VariableAttribute;
import uk.ac.bristol.star.cdf.record.SimpleNioBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class ConvertCDFToCSV extends AbstractProcessor {

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as CDF").build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("The original FlowFile is routed to this relationship after it has been converted to CSV").build();

    static final Relationship REL_GLOBAL = new Relationship.Builder().name("global")
            .description("The original FlowFile is routed to this relationship after it has been converted to CSV").build();

    static final Relationship REL_VAR_INFO = new Relationship.Builder().name("var_info")
            .description("The original FlowFile is routed to this relationship after it has been converted to CSV").build();

    static final Relationship REL_VAR_RECS = new Relationship.Builder().name("var_records")
            .description("The original FlowFile is routed to this relationship after it has been converted to CSV").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_GLOBAL);
        relationships.add(REL_VAR_INFO);
        relationships.add(REL_VAR_RECS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            session.read(flowFile, in -> {
                String uuid = flowFile.getAttribute("uuid");
                String filename = flowFile.getAttribute("filename");

                FlowFile cdfFlowFile = session.create(flowFile);
                FlowFile cdfVarInfoFlowFile = session.create(flowFile);
                FlowFile cdfVarRecFlowFile = session.create(flowFile);
                try {
                    // Setup CDF Reader
                    ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(in));
                    in.close();
                    CdfContent cdfContent = new CdfContent(new CdfReader(new SimpleNioBuf(byteBuffer, true, false)));

//                    cdfFlowFile = session.write(cdfFlowFile, out -> {
//                        try (CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(out), CSVFormat.newFormat('\1').withRecordSeparator('\n'))) {
//                            printer.printRecord("id", "filename", "global_attributes");
//                            CdfInfo cdfInfo = cdfContent.getCdfInfo();
//                            printer.print(flowFile.getAttribute("filename"));
//                            StringBuilder sb = new StringBuilder();
//
//                            GlobalAttribute[] gAtts = cdfContent.getGlobalAttributes();
//                            for (int x = 0; x < gAtts.length; x++) {
//                                if (x > 0) sb.append('\2');
//                                GlobalAttribute gAtt = gAtts[x];
//                                sb.append(gAtt.getName()).append('\3');
//                                AttributeEntry[] attEnts = gAtt.getEntries();
//                                for (int i = 0; i < attEnts.length; i++) {
//                                    AttributeEntry attEnt = attEnts[i];
//                                    if (attEnt != null) {
//                                        if (i > 0) sb.append('\4');
//                                        sb.append(i).append('\5');
//                                        sb.append(attEnt.getDataType().getName()).append('\5');
//                                        sb.append(attEnt.toString());
//                                    }
//                                }
//                            }
//                            printer.print(sb.toString());
//                        }
//                    });

//                    cdfVarInfoFlowFile = session.write(cdfVarInfoFlowFile, out -> {
//                        try (CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(out), CSVFormat.newFormat('\1').withRecordSeparator('\n'))) {
////                            printer.printRecord("id", "variable_name", "type", "attributes");
//
//                            Variable[] vars = cdfContent.getVariables();
//                            VariableAttribute[] varAtts = cdfContent.getVariableAttributes();
//                            StringBuilder sb = new StringBuilder();
//                            int v = 0;
//                            for (Variable var : vars) {
//                                if (v++ > 0) printer.println();
//                                printer.print(uuid);
//                                printer.print(filename);
//                                printer.print(var.getDataType().getName());
//                                for (VariableAttribute varAtt : varAtts) {
//                                    AttributeEntry attEnt = varAtt.getEntry(var);
//                                    if (attEnt != null) {
//                                        sb.append(attEnt.getDataType().getName()).append('\5');
//                                        sb.append(attEnt.toString());
//                                    }
//                                }
//                            }
//                        }
//                    });
                    String varId = cdfVarInfoFlowFile.getAttribute("uuid");
                    cdfVarRecFlowFile = session.write(cdfVarRecFlowFile, out -> {
                        try (CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(out), CSVFormat.newFormat(',').withRecordSeparator('\n'))) {
                            printer.printRecord("file_id","variable_name", "record_number", "element_number", "element_value");

                            Variable[] vars = cdfContent.getVariables();
                            boolean firstRow = true;
                            for (Variable var : vars) {
                                for (int i = 0; i < var.getRecordCount(); i++) {
                                    Object tmpArray = var.createRawValueArray();
                                    Object record = var.readShapedRecord(i, true, tmpArray);

                                    if (record.getClass().isArray()) {
                                        for (int v = 0; v < Array.getLength(record); v++) {
                                            if (!firstRow) printer.println();
                                            printer.print(uuid);
                                            printer.print(var.getName());
                                            printer.print(i);
                                            printer.print(v);
                                            printer.print(Array.get(record, v));
                                            firstRow = false;
                                        }
                                    } else {
                                        if (!firstRow) printer.println();
                                        printer.print(uuid);
                                        printer.print(var.getName());
                                        printer.print(i);
                                        printer.print(0);
                                        printer.print(record);
                                        firstRow = false;
                                    }
                                }
                            }
                        }
                    });
                    session.transfer(cdfFlowFile, REL_GLOBAL);
                    session.transfer(cdfVarInfoFlowFile, REL_VAR_INFO);
                    session.transfer(cdfVarRecFlowFile, REL_VAR_RECS);
                } catch (Exception e) {
                    session.remove(cdfFlowFile);
                    session.remove(cdfVarInfoFlowFile);
                    session.remove(cdfVarRecFlowFile);
                    throw e;
                }
            });
            session.transfer(flowFile, REL_ORIGINAL);
        } catch (Exception e) {
            getLogger().error("Failed to Convert CDF to CSV", e);
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
