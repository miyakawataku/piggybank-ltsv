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

package org.apache.pig.piggybank.test.storage;

import org.apache.pig.piggybank.storage.LTSVLoader;

import org.apache.pig.ExecType;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.LoadPushDown.RequiredFieldResponse;
import org.apache.pig.PigServer;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.apache.pig.test.Util;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.instanceOf;

/**
 * @see LTSVLoader Test target
 */
public class TestLTSVLoader {

    /** Stub server. */
    private final PigServer pigServer;

    public TestLTSVLoader() throws Exception {
        this.pigServer = new PigServer(ExecType.LOCAL);
        this.pigServer.getPigContext().getProperties().setProperty("mapred.map.max.attempts", "1");
        this.pigServer.getPigContext().getProperties().setProperty("mapred.reduce.max.attempts", "1");
    }

    // Tests for jobs, from input to output {{{1

    /**
     * Extracts a map from each line, and use all of the columns.
     */
    @Test
    public void test_extract_map_without_projection() throws Exception {
        String inputFileName = "TestLTSVLoader-test_extract_map_without_projection.ltsv";
        Util.createLocalInputFile(inputFileName, new String[] {
            "id:001\tname:John",
            "id:002\tname:Paul"
        });
        pigServer.registerQuery(String.format(
                    "beatle = LOAD '%s' USING org.apache.pig.piggybank.storage.LTSVLoader();"
                    , inputFileName));
        Iterator<Tuple> it = pigServer.openIterator("beatle");
        checkTuple(it.next(), map("id", byteArray("001"), "name", byteArray("John")));
        checkTuple(it.next(), map("id", byteArray("002"), "name", byteArray("Paul")));
        checkNoMoreTuple(it);
    }

    /**
     * Extracts a map from each line, and use a subset of the columns.
     */
    @Test
    public void test_extract_map_with_projection() throws Exception {
        String inputFileName = "TestLTSVLoader-test_extract_map_with_projection.ltsv";
        Util.createLocalInputFile(inputFileName, new String[] {
            "host:host1.example.org\treq:GET /index.html\tua:Opera/9.80",
            "host:host1.example.org\treq:GET /favicon.ico\tua:Opera/9.80",
            "host:pc.example.com\treq:GET /news.html\tua:Mozilla/5.0"
        });
        pigServer.registerQuery(String.format("access = LOAD '%s'"
                    + " USING org.apache.pig.piggybank.storage.LTSVLoader()"
                    + " AS (m:map[]);"
                    , inputFileName));
        pigServer.registerQuery("host_ua = FOREACH access GENERATE m#'host', m#'ua';");
        Iterator<Tuple> it = pigServer.openIterator("host_ua");
        checkTuple(it.next(), byteArray("host1.example.org"), byteArray("Opera/9.80"));
        checkTuple(it.next(), byteArray("host1.example.org"), byteArray("Opera/9.80"));
        checkTuple(it.next(), byteArray("pc.example.com"), byteArray("Mozilla/5.0"));
        checkNoMoreTuple(it);
    }

    /**
     * Extracts fields from all the columns in a line.
     */
    @Test
    public void test_extract_all_fields() throws Exception {
        String inputFileName = "TestLTSVLoader-test_extract_all_fields.ltsv";
        Util.createLocalInputFile(inputFileName, new String[] {
            "id:001\tname:John",
            "id:002\tname:Paul"
        });
        pigServer.registerQuery(String.format(
                    "beatle = LOAD '%s'"
                    + " USING org.apache.pig.piggybank.storage.LTSVLoader('id:int, name: chararray');"
                    , inputFileName));
        Iterator<Tuple> it = pigServer.openIterator("beatle");
        checkTuple(it.next(), 1, "John");
        checkTuple(it.next(), 2, "Paul");
        checkNoMoreTuple(it);
    }

    /**
     * Extracts fields from a subset of the columns in a line.
     */
    @Test
    public void test_extract_subset_of_fields() throws Exception {
        String inputFileName = "TestLTSVLoader-test_extract_subset_of_fields.ltsv";
        Util.createLocalInputFile(inputFileName, new String[] {
            "host:host1.example.org\treq:GET /index.html\tua:Opera/9.80",
            "host:host1.example.org\treq:GET /favicon.ico\tua:Opera/9.80",
            "host:pc.example.com\treq:GET /news.html\tua:Mozilla/5.0"
        });
        pigServer.registerQuery(String.format("host_ua = LOAD '%s'"
                    + " USING org.apache.pig.piggybank.storage.LTSVLoader("
                    + "    'host:chararray, ua:chararray');"
                    , inputFileName));
        Iterator<Tuple> it = pigServer.openIterator("host_ua");
        checkTuple(it.next(), "host1.example.org", "Opera/9.80");
        checkTuple(it.next(), "host1.example.org", "Opera/9.80");
        checkTuple(it.next(), "pc.example.com", "Mozilla/5.0");
        checkNoMoreTuple(it);
    }

    @Test
    public void test_extract_subset_of_fields_and_do_projection() throws Exception {
        String inputFileName = "TestLTSVLoader-test_extract_subset_of_fields_and_do_projection.ltsv";
        Util.createLocalInputFile(inputFileName, new String[] {
            "host:host1.example.org\treq:GET /index.html\tua:Opera/9.80",
            "host:host1.example.org\treq:GET /favicon.ico\tua:Opera/9.80",
            "host:pc.example.com\treq:GET /news.html\tua:Mozilla/5.0"
        });
        pigServer.registerQuery(String.format("host_ua = LOAD '%s'"
                    + " USING org.apache.pig.piggybank.storage.LTSVLoader("
                    + "    'host:chararray, ua:chararray');"
                    , inputFileName));
        pigServer.registerQuery("ua = FOREACH host_ua GENERATE ua;");
        Iterator<Tuple> it = pigServer.openIterator("ua");
        checkTuple(it.next(), "Opera/9.80");
        checkTuple(it.next(), "Opera/9.80");
        checkTuple(it.next(), "Mozilla/5.0");
        checkNoMoreTuple(it);
    }

    /**
     * Malformed columns are not contained in the output.
     */
    @Test
    public void test_malformed_column_is_skipped() throws Exception {
        String inputFileName = "TestLTSVLoader-test_malformed_column_is_skipped.ltsv";

        // Each line contains 5 malformed columns: "", "", errordataX, "", ""
        Util.createLocalInputFile(inputFileName, new String[] {
            "\t\tid:001\terrordata1\tname:John\t\t",
            "\t\tid:002\terrordata2\tname:Paul\t\t"
        });

        pigServer.registerQuery(String.format(
                    "beatle = LOAD '%s' USING org.apache.pig.piggybank.storage.LTSVLoader();"
                    , inputFileName));

        // The relation "beatle" should contain all the columns except for malformed ones.
        Iterator<Tuple> it = pigServer.openIterator("beatle");
        checkTuple(it.next(), map("id", byteArray("001"), "name", byteArray("John")));
        checkTuple(it.next(), map("id", byteArray("002"), "name", byteArray("Paul")));
        checkNoMoreTuple(it);

        // Malformed columns should be warned.
        Counter warningCounter = PigStatusReporter.getInstance().getCounter(PigWarning.UDF_WARNING_8);
        assertThat(warningCounter.getValue(), is(5L * 2));
    }

    /**
     * Checks that the actual tuple is a tuple of the given elements,
     */
    private void checkTuple(Tuple actual, Object ... expectedElements) throws Exception {
        assertThat(actual, is(Util.createTuple(expectedElements)));
    }

    /**
     * Checks that the iterator has no more tuple.
     */
    private void checkNoMoreTuple(Iterator<Tuple> iterator) {
        assertThat(iterator.hasNext(), is(false));
    }

    /**
     * Returns a map of {args[0]: args[1], args[2]: args[3], ...}.
     */
    private Map<String, Object> map(Object ... args) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (int keyIndex = 0; keyIndex < args.length; keyIndex += 2) {
            int valueIndex = keyIndex + 1;
            map.put((String) args[keyIndex], args[valueIndex]);
        }
        return map;
    }

    /**
     * Encodes the string in UTF-8 to DataByteArray.
     */
    private DataByteArray byteArray(String string) throws Exception {
        return new DataByteArray(string);
    }

    // Tests LTSVLoader.pushProjection() {{{1

    /** Signature of the invocation of the loader in the test case. */
    private final String signature = newSignature();

    /** Loader to test. */
    private final LTSVLoader loader = new LTSVLoader();

    {
        this.loader.setUDFContextSignature(this.signature);
    }

    /**
     * Map keys given to pushProjection() are set as labels to be output.
     */
    @Test
    public void test_map_keys_are_projected() throws Exception {
        RequiredFieldList singletonMapFieldList = fieldList(mapField("host", "ua", "referer"));
        RequiredFieldResponse response = this.loader.pushProjection(singletonMapFieldList);
        checkResponse(response, true);
        checkLabelsToOutput(newSet("host", "ua", "referer"));
    }

    /**
     * No projection is performed if the only field does not contain subfields.
     */
    @Test
    public void test_no_projection_performed_when_subfields_not_given() throws Exception {
        RequiredField mapField = new RequiredField("map", 0, null, DataType.MAP);
        RequiredFieldList withoutSubfields = fieldList(mapField);
        RequiredFieldResponse response = this.loader.pushProjection(withoutSubfields);
        checkResponse(response, false);
        checkLabelsToOutput(null);
    }

    /**
     * FrontendException is thrown when the index of the only field is not zero.
     */
    @Test
    public void test_error_thrown_when_index_of_field_is_nonzero() {
        RequiredField nonZeroIndexField = new RequiredField("map", 1, null, DataType.MAP);
        RequiredFieldList fieldListWithNonZeroIndex = fieldList(nonZeroIndexField);
        try {
            this.loader.pushProjection(fieldListWithNonZeroIndex);
            fail();
        } catch (FrontendException exception) {
            assertThat(exception.getErrorCode(), is(2998));
        }
    }

    /**
     * No projection is performed if no field is specified.
     */
    @Test
    public void test_no_projection_performed_if_no_field_is_specified() throws Exception {
        RequiredFieldList emptyFieldList = fieldList();
        RequiredFieldResponse response = this.loader.pushProjection(emptyFieldList);
        checkResponse(response, false);
        checkLabelsToOutput(null);
    }

    /**
     * FrontendException is thrown when multiple fields are specified.
     */
    @Test
    public void test_no_error_thrown_when_multiple_fields_specified() {
        RequiredField field0 = new RequiredField("map0", 0, null, DataType.MAP);
        RequiredField field1 = new RequiredField("map1", 1, null, DataType.MAP);
        RequiredFieldList multipleElementsFieldList = fieldList(field0, field1);
        try {
            this.loader.pushProjection(multipleElementsFieldList);
            fail();
        } catch (FrontendException exception) {
            assertThat(exception.getErrorCode(), is(2998));
        }
    }

    /**
     * No projection is performed if projection information is not given.
     */
    @Test
    public void test_no_projection_performed_when_fields_not_given() throws Exception {
        RequiredFieldList emptyFieldList = new RequiredFieldList(null);
        RequiredFieldResponse response = this.loader.pushProjection(emptyFieldList);
        checkResponse(response, false);
        checkLabelsToOutput(null);
    }

    /**
     * No projection is performed if the loader is constructed with a schema.
     */
    @Test
    public void test_no_projection_performed_when_schema_are_specified_for_constructor() throws Exception {
        LTSVLoader fieldsLoader = new LTSVLoader("host:chararray, ua:chararray");
        fieldsLoader.setUDFContextSignature(this.signature);
        RequiredField hostField = new RequiredField("host", 0, null, DataType.CHARARRAY);
        RequiredFieldList fieldList = fieldList(hostField);
        RequiredFieldResponse response = fieldsLoader.pushProjection(fieldList);
        checkResponse(response, false);
        checkLabelsToOutput(null);
    }

    /** Holder of a unique number. */
    private static int uniqueNumber = 0;

    /**
     * Makes a new unique signature.
     */
    private static String newSignature() {
        ++ TestLTSVLoader.uniqueNumber;
        return TestLTSVLoader.class.getName() + "#" + TestLTSVLoader.uniqueNumber;
    }

    /**
     * Makes a new set of strings.
     */
    private Set<String> newSet(String ... strings) {
        return new HashSet<String>(Arrays.asList(strings));
    }

    /**
     * Checks that the response of pushProjection() is as expected.
     */
    private void checkResponse(RequiredFieldResponse response, boolean expected) {
        assertThat(response.getRequiredFieldResponse(), is(expected));
    }

    /**
     * Checks that the set of labels to output, which is set by pushProjection();
     * is as expected.
     */
    private void checkLabelsToOutput(Set<String> expectedLabelsToOutput) {
        Set<String> actualLabelsToOutput = getLabelsToOutput();
        assertThat(actualLabelsToOutput, is(expectedLabelsToOutput));
    }

    /**
     * Returns a new field list specifier.
     */
    private RequiredFieldList fieldList(RequiredField ... fields) {
        return new RequiredFieldList(Arrays.asList(fields));
    }

    /**
     * Returns a new specifier of a map field which consists of the given subfield keys.
     */
    private RequiredField mapField(String ... keys) {
        List<RequiredField> subfields = new ArrayList<RequiredField>();
        int index = 0;
        for (String key : keys) {
            RequiredField subfield = new RequiredField(key, index, null, DataType.BYTEARRAY);
            subfields.add(subfield);
            ++ index;
        }
        return new RequiredField(null, 0, subfields, DataType.MAP);
    }

    /**
     * Returns the set of lables to output, which is set by pushProjection().
     */
    private Set<String> getLabelsToOutput() {
        UDFContext context = UDFContext.getUDFContext();
        Properties props = context.getUDFProperties(LTSVLoader.class,  new String[] { this.signature });
        @SuppressWarnings("unchecked")
        Set<String> labelsToOutput = (Set<String>) props.get("LABELS_TO_OUTPUT");
        return labelsToOutput;
    }

    // Tests LTSVLoader.getInputFormat() {{{1

    /**
     * PigTextInputFormat is used for *.txt.
     */
    @Test
    public void test_regular_input_format_is_used_for_txt_file() throws Exception {
        Job job = new Job();

        // Touches XXXX.txt, and sets the file as the input location.
        String textFileName = "TestLTSVLoader-test_regular_input_format_is_used_for_text_file.txt";
        Util.createLocalInputFile(textFileName, new String[0]);
        this.loader.setLocation(textFileName, job);

        InputFormat inputFormat = this.loader.getInputFormat();
        assertThat(inputFormat, instanceOf(PigTextInputFormat.class));
    }

    /**
     * PigTextInputFormat is used for a directory.
     */
    @Test
    public void test_regular_input_format_is_used_for_directory() throws Exception {
        Job job = new Job();

        // Mkdir XXXX, and sets the directory as the input location.
        String dirName = "TestLTSVLoader-test_regular_input_format_is_used_for_directory";
        makeLocalDirectory(dirName);
        this.loader.setLocation(dirName, job);

        InputFormat inputFormat = this.loader.getInputFormat();
        assertThat(inputFormat, instanceOf(PigTextInputFormat.class));
    }

    /**
     * PigTextInputFormat is used for *.gz.
     */
    @Test
    public void test_regular_input_format_is_used_for_gz_file() throws Exception {
        Job job = new Job();

        // Touches XXXX.gz, and sets the file as the input location.
        String gzFileName = "TestLTSVLoader-test_regular_input_format_is_used_for_gzip_file.gz";
        Util.createLocalInputFile(gzFileName, new String[0]);
        this.loader.setLocation(gzFileName, job);

        InputFormat inputFormat = this.loader.getInputFormat();
        assertThat(inputFormat, instanceOf(PigTextInputFormat.class));
    }

    /**
     * Bzip2TextInputFormat is used for *.bz.
     */
    @Test
    public void test_bzip2_input_format_is_used_for_bz_file() throws Exception {
        Job job = new Job();

        // Touches XXXX.bz, and sets the file as the input location.
        String bzFileName = "TestLTSVLoader-test_bzip2_input_format_is_used_for_bz_file.bz";
        Util.createLocalInputFile(bzFileName, new String[0]);
        this.loader.setLocation(bzFileName, job);

        InputFormat inputFormat = this.loader.getInputFormat();
        assertThat(inputFormat, instanceOf(Bzip2TextInputFormat.class));
    }

    /**
     * Bzip2TextInputFormat is used for *.bz2.
     */
    @Test
    public void test_bzip2_input_format_is_used_for_bz2_file() throws Exception {
        Job job = new Job();

        // Touches XXXX.bz2, and sets the file as the input location.
        String bz2FileName = "TestLTSVLoader-test_bzip2_input_format_is_used_for_bz2_file.bz2";
        Util.createLocalInputFile(bz2FileName, new String[0]);
        this.loader.setLocation(bz2FileName, job);

        InputFormat inputFormat = this.loader.getInputFormat();
        assertThat(inputFormat, instanceOf(Bzip2TextInputFormat.class));
    }

    /**
     * Creates a local directory.
     */
    private void makeLocalDirectory(String pathname) throws IOException {
        File dir = new File(pathname);
        dir.deleteOnExit();
        boolean created = dir.mkdir();
        if (! created) {
            throw new IOException("Cannot create a directory: " + pathname);
        }
    }

    // }}}
}

// vim: et sw=4 sts=4 fdm=marker
