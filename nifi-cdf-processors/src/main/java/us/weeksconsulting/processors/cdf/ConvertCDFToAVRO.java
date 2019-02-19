package us.weeksconsulting.processors.cdf;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import uk.ac.bristol.star.cdf.AttributeEntry;
import uk.ac.bristol.star.cdf.CdfContent;
import uk.ac.bristol.star.cdf.CdfReader;
import uk.ac.bristol.star.cdf.Variable;
import uk.ac.bristol.star.cdf.VariableAttribute;
import uk.ac.bristol.star.cdf.record.SimpleNioBuf;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConvertCDFToAVRO extends AbstractProcessor {
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as CDF").build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as CDF").build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("The original FlowFile is routed to this relationship after it has been converted to CSV").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    private Schema cdfVarSchema;
    private Schema cdfVarAttSchema;
    private Schema cdfVarRecSchema;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<String> nullStringList = new ArrayList<>();

        cdfVarAttSchema = SchemaBuilder
                .record("cdfVarAtt")
                .fields()
                .name("attribute_type").type().stringType().noDefault()
                .name("attribute_value").type().stringType().noDefault()
                .endRecord();

        cdfVarSchema = SchemaBuilder
                .record("cdfVar")
//                .namespace("org.apache.hive")
                .fields()
                .name("file_id").type().stringType().noDefault()
                .name("variable_name").type().stringType().noDefault()
                .name("variable_type").type().stringType().noDefault()
//                .name("num_elements").type().intType().noDefault()
//                .name("dim").type().intType().noDefault()
//                .name("dim_sizes").type().array().items().intType().noDefault()
//                .name("dim_variances").type().array().items().booleanType().noDefault()
//                .name("rec_variance").type().intType().noDefault()
//                .name("max_records").type().intType().noDefault()
                .name("attributes").type().map().values(cdfVarAttSchema).noDefault()
                .endRecord();


        cdfVarRecSchema = SchemaBuilder
                .record("cdfVarRec")
//                .namespace("org.apache.hive")
                .fields()
                .name("file_id").type().stringType().noDefault()
                .name("variable_name").type().stringType().noDefault()
                .name("variable_type").type().stringType().noDefault()
                .name("record_number").type().intType().noDefault()
                .name("record_count").type().intType().noDefault()
                .name("record_array").type().array().items().stringType().noDefault()
                .endRecord();

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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.read(flowFile, in -> {
            FlowFile cdfRecFlowFile = session.create(flowFile);
            FlowFile cdfVarFlowFile = session.create(flowFile);

            session.putAttribute(cdfVarFlowFile, "cdf_extract_type", "cdfVarFlowFile");
            session.putAttribute(cdfRecFlowFile, "cdf_extract_type", "cdfRecFlowFile");

            // Setup CDF Reader
            ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(in));
            in.close();
            CdfContent cdfContent = new CdfContent(new CdfReader(new SimpleNioBuf(byteBuffer, true, false)));


            DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter());
            writer.setCodec(CodecFactory.deflateCodec(9));

            cdfVarFlowFile = session.write(cdfVarFlowFile, out -> {
                DataFileWriter<Record> w = writer.create(cdfVarSchema, out);

                Variable[] vars = cdfContent.getVariables();
                VariableAttribute[] varAtts = cdfContent.getVariableAttributes();

                Record r = new GenericData.Record(cdfVarSchema);

                for (Variable var : vars) {

                    r.put("file_id", flowFile.getAttribute("uuid"));
                    r.put("variable_name", var.getName());
                    r.put("variable_type", var.getDataType().getName());

                    Map<String, Record> attMap = new HashMap<>();
                    for (VariableAttribute varAtt : varAtts) {
                        AttributeEntry attEnt = varAtt.getEntry(var);
                        if (attEnt != null) {
                            Record x = new GenericData.Record(cdfVarAttSchema);
                            x.put("attribute_type", attEnt.getDataType().getName());
                            x.put("attribute_value", attEnt.toString());
                            attMap.put(varAtt.getName(), x);
                        }
                    }
                    r.put("attributes", attMap);
                    w.append(r);
                }
                w.close();
            });

            cdfRecFlowFile = session.write(cdfRecFlowFile, out -> {
                DataFileWriter<Record> w = writer.create(cdfVarRecSchema, out);

                Variable[] vars = cdfContent.getVariables();
                for (Variable var : vars) {
                    for (int i = 0; i < var.getRecordCount(); i++) {
                        Record r = new GenericData.Record(cdfVarRecSchema);
                        Object tmpArray = var.createRawValueArray();
                        Object record = var.readShapedRecord(i, true, tmpArray);

                        r.put("file_id", flowFile.getAttribute("uuid"));
                        r.put("variable_name", var.getName());
                        r.put("variable_type", var.getDataType().getName());
                        r.put("record_number", i);

                        List<String> recordArray = new ArrayList<>();
                        if (record.getClass().isArray()) {
                            int arraySize = Array.getLength(record);
                            r.put("record_count", arraySize);
                            for (int v = 0; v < arraySize; v++) {
                                recordArray.add(Array.get(record, v).toString());
                            }
                        } else {
                            r.put("record_count", 1);
                            recordArray.add(record.toString());
                        }
                        r.put("record_array", recordArray);
                        w.append(r);
                    }
                }
                w.close();
            });
            writer.close();

            session.transfer(cdfVarFlowFile, REL_SUCCESS);
//            session.transfer(cdfRecFlowFile, REL_SUCCESS);
            session.remove(cdfRecFlowFile);
        });

        session.transfer(flowFile, REL_ORIGINAL);
    }
}
