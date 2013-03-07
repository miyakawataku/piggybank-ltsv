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

package org.apache.pig.piggybank.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Collections;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.ResourceSchema;
import org.apache.pig.PigWarning;
import org.apache.pig.PigException;
import org.apache.pig.LoadMetadata;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.UDFContext;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Loader UDF for <a href="http://ltsv.org/">LTSV</a> files,
 * or Labeled Tab-separated Values files.
 *
 * <h3>About LTSV</h3>
 *
 * <p>
 * LTSV, or Labeled Tab-separated Values format is a format for log files.
 * LTSV is based on TSV.
 * Columns are separated by tab characters,
 * and each of columns includes a label and a value,
 * separated by a ":" character.
 * </p>
 *
 * <p>
 * This is an example LTSV log file.
 * Suppose that columns are separated by tab characters.
 * </p>
 *
 * <pre>
 * host:host1.example.org   req:GET /index.html   ua:Opera/9.80
 * host:host1.example.org   req:GET /favicon.ico  ua:Opera/9.80
 * host:pc.example.com      req:GET /news.html    ua:Mozilla/5.0
 * </pre>
 *
 * <p>You can read about LTSV on <a href="http://ltsv.org/">http://ltsv.org/</a>.</p>
 *
 * <p>
 * You can use the UDF in two ways.
 * First, you can specify labels and get them as Pig fields.
 * Second, you can extract a map of all columns of each LTSV line.
 * </p>
 *
 * <h3>Extract fields from each line</h3>
 *
 * <p>
 * To extract fields from each line,
 * construct a loader function with a schema string.
 * </p>
 *
 * <dl>
 *   <dt><em>Syntax</em></dt>
 *   <dd>org.apache.pig.piggybank.storage.LTSVLoader('&lt;schema-string&gt;')</dd>
 *
 *   <dt><em>Output</em></dt>
 *   <dd>The schema of the output is specified by the argument of the constructor.
 *   The value of a field comes from a column whoes label is equal to the field name.
 *   If the corresponding label does not exist in the LTSV line,
 *   the field is set to null.</dd>
 * </dl>
 *
 * <p>
 * This example script loads an LTSV file shown in the previous section, named access.log.
 * </p>
 *
 * <pre>
 * -- Parses the access log and count the number of lines
 * -- for each pair of the host column and the ua column.
 * access = LOAD 'access.log' USING org.apache.pig.piggybank.storage.LTSVLoader('host:chararray, ua:chararray');
 * grouped_access = GROUP access BY (host, ua);
 * count_for_host_ua = FOREACH grouped_access GENERATE group.host, group.ua, COUNT(access);
 *
 * -- Prints out the result.
 * DUMP count_for_host_ua;
 * </pre>
 *
 * <p>The below text will be printed out.</p>
 *
 * <pre>
 * (host1.example.org,Opera/9.80,2)
 * (pc.example.com,Firefox/5.0,1)
 * </pre>
 *
 * <h3>Extract a map from each line</h3>
 *
 * <p>
 * To extract a map from each line,
 * construct a loader function without parameters.
 * </p>
 *
 * <dl>
 *   <dt><em>Syntax</em></dt>
 *   <dd>org.apache.pig.piggybank.storage.LTSVLoader()</dd>
 *
 *   <dt><em>Output</em></dt>
 *   <dd>The schema of the output is (:map[]), or an unary tuple of a map, for each LTSV row.
 *   The key of a map is a label of the LTSV column.
 *   The value of a map comes from characters after ":" in the LTSV column.</dd>
 * </dl>
 *
 * <p>
 * This example script loads an LTSV file shown in the previous section, named access.log.
 * </p>
 *
 * <pre>
 * -- Parses the access log and projects the user agent field.
 * access = LOAD 'access.log' USING org.apache.pig.piggybank.storage.LTSVLoader() AS (m);
 * user_agent = FOREACH access GENERATE m#'ua' AS ua;
 * -- Prints out the user agent for each access.
 * DUMP user_agent;
 * </pre>
 *
 * <p>The below text will be printed out.</p>
 *
 * <pre>
 * (Opera/9.80)
 * (Opera/9.80)
 * (Firefox/5.0)
 * </pre>
 *
 * <h3>Handling of malformed input</h3>
 *
 * <p>
 * All valid LTSV columns contain ":" to separate a field and a value.
 * If a column does not contain ":", the column is malformed.
 * </p>
 *
 * <p>
 * For each of malformed column, the counter
 * {@link PigWarning#UDF_WARNING_8} will be counted up.
 * This counter is printed out when the Pig job completes.
 * </p>
 *
 * <p>
 * In the mapreduce mode, the counter shall be output like below.
 * </p>
 *
 * <pre>
 * 2013-02-17 12:14:22,070 [main] WARN  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher - Encountered Warning UDF_WARNING_8 10000 time(s).
 * </pre>
 *
 * <p>
 * For the first 100 of malformed columns in each task,
 * warning messages are written to the tasklog.
 * Such as:
 * </p>
 *
 * <pre>
 * 2013-02-17 12:13:42,142 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata1" does not contain ":".
 * 2013-02-17 12:13:42,158 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata2" does not contain ":".
 * 2013-02-17 12:13:42,158 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata3" does not contain ":".
 * 2013-02-17 12:13:42,158 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata4" does not contain ":".
 * 2013-02-17 12:13:42,158 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata5" does not contain ":".
 * 2013-02-17 12:13:42,159 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata6" does not contain ":".
 * 2013-02-17 12:13:42,159 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata7" does not contain ":".
 * 2013-02-17 12:13:42,159 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata8" does not contain ":".
 * 2013-02-17 12:13:42,159 WARN org.apache.pig.piggybank.storage.LTSVLoader: MalformedColumn: Column "errordata9" does not contain ":".
 * ...
 * </pre>
 */
public class LTSVLoader extends FileInputLoadFunc implements LoadPushDown, LoadMetadata {

    /** Logger of this class. */
    private static final Log LOG = LogFactory.getLog(LTSVLoader.class);

    /** Emitter of tuples */
    private final TupleEmitter tupleEmitter;

    /** Length of "\t" in UTF-8. */
    private static int TAB_LENGTH = 1;

    /** Length of ":" in UTF-8. */
    private static int COLON_LENGTH = 1;

    /** Factory of tuples. */
    private final TupleFactory tupleFactory = TupleFactory.getInstance();

    /** Schema of the output of the loader, which is (:map[]). */
    private static final ResourceSchema MAP_SCHEMA
        = new ResourceSchema(new Schema(new Schema.FieldSchema(null, DataType.MAP)));

    /**
     * An error code of an input error.
     * See https://cwiki.apache.org/confluence/display/PIG/PigErrorHandlingFunctionalSpecification.
     */
    private static final int INPUT_ERROR_CODE = 6018;

    /**
     * An error message of an input error.
     * See https://cwiki.apache.org/confluence/display/PIG/PigErrorHandlingFunctionalSpecification.
     */
    private static final String INPUT_ERROR_MESSAGE = "Error while reading input";

    /** Underlying record reader of a text file. */
    @SuppressWarnings("rawtypes")
    private RecordReader reader = null;

    /** Sequence number of the next log message, which starts from 0. */
    private int warnLogSeqNum = 0;

    /** Max count of warning logs which will be output to the task log. */
    private static final int MAX_WARN_LOG_COUNT = 100;

    /** Key of a property which contains Set[String] of labels to output. */
    private static final String LABELS_TO_OUTPUT = "LABELS_TO_OUTPUT";

    /**
     * An error code of an internal error.
     * See https://cwiki.apache.org/confluence/display/PIG/PigErrorHandlingFunctionalSpecification.
     */
    private static final int INTERNAL_ERROR_CODE = 2998;

    /** Location of input files. */
    private String loadLocation;

    /** Signature of the UDF invocation, used to get UDFContext. */
    private String signature;

    /**
     * Constructs a loader which extracts a tuple including a single map
     * for each column of an LTSV line.
     */
    public LTSVLoader() {
        this.tupleEmitter = new MapTupleEmitter();
    }

    /**
     * Constructs a loader which extracts a tuple including specified fields
     * for each column of an LTSV line.
     *
     * @param schemaString
     *     Schema of fields to extract.
     *
     * @throws IOException
     *     Thrown when an I/O error occurs during construction.
     */
    public LTSVLoader(String schemaString) throws IOException {
        this.tupleEmitter = new FieldsTupleEmitter(schemaString);
    }

    /**
     * Reads an LTSV line and returns a tuple,
     * or returns {@code null} if the loader reaches the end of the block.
     *
     * @return
     *     Tuple corresponding to an LTSV line,
     *     or {@code null} if the loader reaches the end of the block.
     *
     * @throws IOException
     *     If an I/O error occurs while reading the line.
     */
    @Override
    public Tuple getNext() throws IOException {
        Text line = readLine();

        if (line == null) {
            return null;
        }

        // Current line: lineBytes[0, lineLength)
        byte[] lineBytes = line.getBytes();
        int lineLength = line.getLength();

        // Reads an map entry from each column.
        int startOfColumn = 0;
        while (startOfColumn <= lineLength) {
            int endOfColumn = findUntil((byte) '\t', lineBytes, startOfColumn, lineLength);
            readColumn(lineBytes, startOfColumn, endOfColumn);
            startOfColumn = endOfColumn + TAB_LENGTH;
        }

        return this.tupleEmitter.emitTuple();
    }

    /**
     * Reads a column to the tuple emitter.
     */
    private void readColumn(byte[] bytes, int start, int end) throws IOException {
        int colon = findUntil((byte) ':', bytes, start, end);
        boolean isLabeled = colon < end;
        if (! isLabeled) {
            warnMalformedColumn(Text.decode(bytes, start, end - start));
            return;
        }

        // Label: bytes[start, colon)
        // Colon: bytes[colon, colon + 1)
        // Value: bytes[colon + 1 = startOfValue, end)

        String label = Text.decode(bytes, start, colon - start);
        int startOfValue = colon + COLON_LENGTH;
        this.tupleEmitter.addColumn(label, bytes, startOfValue, end);
    }

    /**
     * Returns the index of the first target in bytes[start, end),
     * or the index of the end.
     */
    private static int findUntil(byte target, byte[] bytes, int start, int end) {
        for (int index = start; index < end; ++ index) {
            if (bytes[index] == target) {
                return index;
            }
        }
        return end;
    }

    /** Outputs a warning for a malformed column. */
    private void warnMalformedColumn(String column) {
        String message = String.format("MalformedColumn: Column \"%s\" does not contain \":\".", column);
        warn(message, PigWarning.UDF_WARNING_8);

        // Output at most MAX_WARN_LOG_COUNT warning messages.
        if (this.warnLogSeqNum < MAX_WARN_LOG_COUNT) {
            LOG.warn(message);
            ++ this.warnLogSeqNum;
        }
    }

    /**
     * Reads a line from the block,
     * or {@code null} if the loader reaches the end of the block.
     */
    private Text readLine() throws IOException {
        try {
            if (! this.reader.nextKeyValue()) {
                return null;
            }

            return (Text) this.reader.getCurrentValue();
        } catch (InterruptedException exception) {
            throw new ExecException(INPUT_ERROR_MESSAGE, INPUT_ERROR_CODE,
                    PigException.REMOTE_ENVIRONMENT, exception);
        }
    }

    /**
     * Constructs a tuple from columns and emits it.
     *
     * This interface is used to switch the output type between a map and fields.
     */
    private interface TupleEmitter {

        /**
         * Adds a value from bytes[startOfValue, endOfValue), corresponding to the label,
         * if the columns is required for the output.
         *
         * @param label
         *     Label of the column.
         *
         * @param bytes
         *     Byte array including the value.
         *
         * @param startOfValue
         *     Index a the start of the value (inclusive).
         *
         * @param endOfValue
         *     Index a the end of the value (exclusive).
         */
        void addColumn(String label, byte[] bytes, int startOfValue, int endOfValue)
            throws IOException;

        /**
         * Emits a tuple and reinitialize the state of the emitter.
         */
        Tuple emitTuple();

        /**
         * Returns the schema of tuples.
         */
        ResourceSchema getSchema();

        /**
         * Notifies required fields in the script.
         *
         * <p>Delegation target from {@link LTSVLoader#pushProjection}.</p>
         */
        RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
            throws FrontendException;
    }

    /**
     * Reads columns and emits a tuple with a single map field (:map[]).
     */
    private class MapTupleEmitter implements TupleEmitter {

        /** Contents of the single map field. */
        private Map<String, Object> map = new HashMap<String, Object>();

        @Override
        public void addColumn(String label, byte[] bytes, int startOfValue, int endOfValue)
        throws IOException {
            if (shouldOutput(label)) {
                DataByteArray value = new DataByteArray(bytes, startOfValue, endOfValue);
                this.map.put(label, value);
            }
        }

        @Override
        public Tuple emitTuple() {
            Tuple tuple = tupleFactory.newTuple(map);
            this.map = new HashMap<String, Object>();
            return tuple;
        }

        /**
         * Returns {@code true} if the column should be output.
         */
        private boolean shouldOutput(String label) {
            boolean outputsEveryColumn = (labelsToOutput() == null);
            return outputsEveryColumn || labelsToOutput().contains(label);
        }

        /** True if {@link labelsToOutput} is initialized. */
        private boolean isProjectionInitialized;

        /**
         * Labels of columns to output, or {@code null} if all columns should be output.
         * This field should be accessed from {@link #labelsToOutput}.
         */
        private Set<String> labelsToOutput;

        /**
         * Returns labels of columns to output,
         * or {@code null} if all columns should be output.
         */
        private Set<String> labelsToOutput() {
            if (! this.isProjectionInitialized) {
                @SuppressWarnings("unchecked")
                Set<String> labels = (Set<String>) getProperties().get(LABELS_TO_OUTPUT);
                this.labelsToOutput = labels;
                LOG.debug("Labels to output: " + this.labelsToOutput);
                this.isProjectionInitialized = true;
            }
            return this.labelsToOutput;
        }

        @Override
        public ResourceSchema getSchema() {
            return MAP_SCHEMA;
        }

        @Override
        public RequiredFieldResponse
        pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
            List<RequiredField> fields = requiredFieldList.getFields();
            if (fields == null || fields.isEmpty()) {
                LOG.debug("No fields specified as required.");
                return new RequiredFieldResponse(false);
            }

            if (fields.size() != 1) {
                String message = String.format(
                        "The loader expects at most one field but %d fields are specified."
                        , fields.size());
                throw new FrontendException(message, INTERNAL_ERROR_CODE, PigException.BUG);
            }

            RequiredField field = fields.get(0);
            if (field.getIndex() != 0) {
                String message = String.format(
                        "The loader produces only 1ary tuples, but the index %d is specified."
                        , field.getIndex());
                throw new FrontendException(message, INTERNAL_ERROR_CODE, PigException.BUG);
            }

            List<RequiredField> mapKeys = field.getSubFields();
            if (mapKeys == null) {
                LOG.debug("All the labels are required.");
                return new RequiredFieldResponse(false);
            }

            Set<String> labels = new HashSet<String>();
            for (RequiredField mapKey : mapKeys) {
                labels.add(mapKey.getAlias());
            }
            getProperties().put(LABELS_TO_OUTPUT, labels);
            LOG.debug("Labels to output: " + labels);
            return new RequiredFieldResponse(true);
        }
    }

    /**
     * Reads columns and emits a tuple with fields specified
     * by the constructor of the load function.
     */
    private class FieldsTupleEmitter implements TupleEmitter {

        /** Schema of tuples. */
        private final ResourceSchema schema;

        /** Tuple to emit. */
        private Tuple tuple;

        /** Caster of values. */
        private final LoadCaster loadCaster = getLoadCaster();

        /** Mapping from labels to indexes in a tuple. */
        private final Map<String, Integer> labelToIndex = new HashMap<String, Integer>();

        /** Set of labels which have occurred and been skipped. */
        private final Set<String> skippedLabels = new HashSet<String>();

        /**
         * Constructs an emitter with the schema.
         */
        private FieldsTupleEmitter(String schemaString) throws IOException {
            Schema rawSchema = Utils.getSchemaFromString(schemaString);
            this.schema = new ResourceSchema(rawSchema);
            for (int index = 0; index < schema.getFields().length; ++ index) {
                this.labelToIndex.put(this.schema.getFields()[index].getName(), index);
            }
            this.tuple = tupleFactory.newTuple(this.schema.getFields().length);
        }

        @Override
        public void addColumn(String label, byte[] bytes, int startOfValue, int endOfValue)
        throws IOException {
            if (! this.labelToIndex.containsKey(label)) {
                logSkippedLabelAtFirstOccurrence(label);
                return;
            }

            int index = this.labelToIndex.get(label);
            ResourceSchema.ResourceFieldSchema fieldSchema = this.schema.getFields()[index];
            int valueLength = endOfValue - startOfValue;
            byte[] valueBytes = new byte[valueLength];
            System.arraycopy(bytes, startOfValue, valueBytes, 0, valueLength);
            Object value = CastUtils.convertToType(this.loadCaster, valueBytes, fieldSchema, fieldSchema.getType());
            this.tuple.set(index, value);
        }

        /**
         * Outputs the label of a skipped column to the task log at the first occurrence.
         */
        private void logSkippedLabelAtFirstOccurrence(String label) {
            if (LOG.isDebugEnabled() && ! this.skippedLabels.contains(label)) {
                this.skippedLabels.add(label);
                LOG.debug("Skipped label: " + label);
            }
        }

        @Override
        public Tuple emitTuple() {
            Tuple resultTuple = this.tuple;
            this.tuple = tupleFactory.newTuple(this.schema.getFields().length);
            return resultTuple;
        }

        @Override
        public ResourceSchema getSchema() {
            return schema;
        }

        @Override
        public RequiredFieldResponse
        pushProjection(RequiredFieldList requiredFieldList) {
            return new RequiredFieldResponse(false);
        }
    }

    /**
     * Saves the RecordReader.
     *
     * @param reader
     *     RecordReader to read LTSV lines from.
     *
     * @param split
     *     Ignored.
     *
     */
    @Override
    public void
    prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    /**
     * Extracts information about which labels are required in the script.
     *
     * @param requiredFieldList
     *     {@inheritDoc}.
     *
     * @return
     *     {@code new RequiredFieldResponse(true)} if projection will be performed,
     *     or {@code new RequiredFieldResponse(false)} otherwise.
     *
     * @throws FrontendException
     *     When an unexpected internal error occurs.
     */
    @Override
    public RequiredFieldResponse
    pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        return this.tupleEmitter.pushProjection(requiredFieldList);
    }

    /**
     * <p>This UDF supports
     * {@linkplain org.apache.pig.LoadPushDown.OperatorSet#PROJECTION projection push-down}.</p>
     *
     * @return
     *     Singleton list of
     *     {@link org.apache.pig.LoadPushDown.OperatorSet#PROJECTION}.
     */
    @Override
    public List<OperatorSet> getFeatures() {
        return Collections.singletonList(OperatorSet.PROJECTION);
    }

    /**
     * Configures the underlying input format
     * to read lines from {@code location}.
     *
     * @param location
     *     Location of LTSV file(s).
     *
     * @param job
     *     Job.
     *
     * @throws IOException
     *     If an I/O error occurs while configuring the job.
     */
    @Override
    public void setLocation(String location, Job job) throws IOException {
        this.loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }

    /**
     * Makes an instance of an input format from which LTSV lines are read.
     *
     * @return
     *     Instance of {@link PigTextInputFormat}.
     */
    @Override
    public InputFormat getInputFormat() {
        if (loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            // Bzip2TextInputFormat can split bzipped files.
            LOG.debug("Uses Bzip2TextInputFormat");
            return new Bzip2TextInputFormat();
        } else {
            LOG.debug("Uses PigTextInputFormat");
            return new PigTextInputFormat();
        }
    }

    /**
     * Saves the signature of the current invocation of the UDF.
     *
     * @param signature
     *     Signature of the current invocation of the UDF.
     */
    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature;
    }

    /**
     * Returns the properties related with the current invocation of the UDF.
     */
    private Properties getProperties() {
        return UDFContext.getUDFContext().getUDFProperties(
                getClass(), new String[] { this.signature });
    }

    // Methods for LoadMetadata

    /**
     * Does nothing,
     * because the loader does not know about partitioning.
     *
     * @param partitionFilter
     *     Ignored.
     */
    @Override
    public void setPartitionFilter(Expression partitionFilter) {
    }

    /**
     * Returns {@code null},
     * because the loader does not know about partitioning.
     *
     * @param location
     *     Ignored.
     *
     * @param job
     *     Ignored.
     *
     * @return
     *     null.
     */
    @Override
    public String[] getPartitionKeys(String location, Job job) {
        return null;
    }

    /**
     * Returns {@code null}, because no statistics are available.
     *
     * @param location
     *     Ignored.
     *
     * @param job
     *     Ignored.
     *
     * @return
     *     null.
     */
    @Override
    public ResourceStatistics getStatistics(String location, Job job) {
        return null;
    }

    /**
     * Returns the schema of the output of the loader.
     *
     * @param location
     *     Ignored, because the schema is constant.
     *
     * @param job
     *     Ignored, because the schema is constant.
     *
     * @return
     *     Schema of the output of the loader.
     */
    @Override
    public ResourceSchema getSchema(String location, Job job) {
        return this.tupleEmitter.getSchema();
    }
}

// vim: et sw=4 sts=4
