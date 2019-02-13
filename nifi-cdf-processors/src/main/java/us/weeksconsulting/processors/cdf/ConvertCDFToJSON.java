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
import org.apache.nifi.processor.io.StreamCallback;
import uk.ac.bristol.star.cdf.AttributeEntry;
import uk.ac.bristol.star.cdf.CdfContent;
import uk.ac.bristol.star.cdf.CdfReader;
import uk.ac.bristol.star.cdf.GlobalAttribute;
import uk.ac.bristol.star.cdf.Variable;
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

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ConvertCDFToJSON extends AbstractProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("A FlowFile is routed to this relationship after it has been converted to JSON").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description(
            "A FlowFile is routed to this relationship if it cannot be parsed as CDF or cannot be converted to JSON for any reason")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
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
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                // Setup CDF Reader
                CdfContent cdfContent;
                byte[] bytes = IOUtils.toByteArray(in);
                in.close();
                ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                CdfReader cdfReader = new CdfReader(new SimpleNioBuf(byteBuffer, true, false));
                cdfContent = new CdfContent(cdfReader);
                JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
                writer.setIndent("  ");

                writer.beginObject();

                JsonWriter gAttsWriter = writer.name("globalAttributes");
                GlobalAttribute[] gAtts = cdfContent.getGlobalAttributes();
                gAttsWriter.beginArray();
                for (GlobalAttribute gAtt : gAtts) {
                    gAttsWriter.beginObject();

                    gAttsWriter.name("name").value(gAtt.getName());
                    JsonWriter gAttWriter = gAttsWriter.name("entries");
                    gAttWriter.beginArray();
                    for (int i = 0; i < gAtt.getEntries().length; i++) {
                        AttributeEntry entry = gAtt.getEntries()[i];
                        if (entry != null) {
                            gAttsWriter.beginObject();
                            gAttWriter.name("entryNum").value(i);
                            gAttWriter.name("entryType").value(entry.getDataType().getName());
                            gAttWriter.name("entryValue").value(entry.toString());
                            gAttsWriter.endObject();
                        }
                    }
                    gAttWriter.endArray();

                    gAttsWriter.endObject();
                }
                gAttsWriter.endArray();

                JsonWriter varsWriter = writer.name("variables");
                Variable[] vars = cdfContent.getVariables();
                varsWriter.beginArray();
                for (Variable var : vars) {
                    varsWriter.beginObject();
                    varsWriter.name("pos").value(var.getNum());
                    varsWriter.name("name").value(var.getName());
                    varsWriter.name("type").value(var.getDataType().getName());
                    JsonWriter recWriter = varsWriter.name("records");
                    recWriter.beginArray();

                    Object tmpRecArray = var.createRawValueArray();
                    for (int i = 0; i < var.getRecordCount(); i++) {
                        Object shapedRec = var.readShapedRecord(i, true, tmpRecArray);
                        recWriter.beginObject();
                        recWriter.name("recNum").value(i);
                        JsonWriter recValueWriter = recWriter.name("recValue");
                        recValueWriter.beginArray();
                        if (shapedRec.getClass().isArray()) {
                            for (int x = 0; x < Array.getLength(shapedRec); x++) {
                                recValueWriter.value(Array.get(shapedRec, x).toString());
                            }
                        } else {
                            recValueWriter.value(shapedRec.toString());
                        }
                        recValueWriter.endArray();
                        recWriter.endObject();
                    }

                    recWriter.endArray();
                    varsWriter.endObject();
                }
                varsWriter.endArray();

                writer.endObject();

                writer.close();
                out.close();
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
    }
}
