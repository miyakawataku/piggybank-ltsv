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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.apache.pig.test.Util;

import org.apache.hadoop.mapreduce.Counter;

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

import static org.hamcrest.CoreMatchers.is;

/**
 * @see LTSVLoader Test target
 */
public class TestLTSVLoader {


    /** Stub server. */
    private final PigServer pigServer;


    public TestLTSVLoader() throws Exception {
        this.pigServer = new PigServer(ExecType.LOCAL, new Properties());
        this.pigServer.getPigContext().getProperties().setProperty("mapred.map.max.attempts", "1");
        this.pigServer.getPigContext().getProperties().setProperty("mapred.reduce.max.attempts", "1");
    }



    // Tests jobs, from input to output


    /** Extracts a map from each line, and use all of the columns. */
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
        assertThat(it.hasNext(), is(false));
    }


    /** Extracts a map from each line, and use a subset of the columns. */
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
        assertThat(it.hasNext(), is(false));
    }


    /** Extracts fields from all the columns in a line. */
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
        assertThat(it.hasNext(), is(false));
    }


    /** Extracts fields from a subset of the columns in a line. */
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
        assertThat(it.hasNext(), is(false));
    }


    /** Malformed columns are not contained in the output. */
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
        assertThat(it.hasNext(), is(false));

        // Malformed columns should be warned.
        Counter warningCounter = PigStatusReporter.getInstance().getCounter(PigWarning.UDF_WARNING_8);
        assertThat(warningCounter.getValue(), is(5L * 2));
    }


    /**
     * Asserts that the actual tuple is a tuple of the given elements,
     */
    private void checkTuple(Tuple actual, Object ... expectedElements) throws Exception {
        assertThat(actual, is(Util.createTuple(expectedElements)));
    }


    /**
     * Returns a map of {args[0]: args[1], args[2]: args[3], ...}.
     */
    private Map<Object, Object> map(Object ... args) {
        Map<Object, Object> map = new HashMap<Object, Object>();
        for (int keyIndex = 0; keyIndex < args.length; keyIndex += 2) {
            int valueIndex = keyIndex + 1;
            map.put(args[keyIndex], args[valueIndex]);
        }
        return map;
    }


    /**
     * Encodes the string in UTF-8 to DataByteArray.
     */
    private DataByteArray byteArray(String string) throws Exception {
        return new DataByteArray(string);
    }


    // Tests LTSVLoader.pushProjection


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
     * No projection is performed if projection information is not given.
     */
    @Test
    public void test_no_projection_information_for_pushProjection() throws Exception {
        RequiredFieldList emptyFieldList = new RequiredFieldList(null);
        RequiredFieldResponse response = this.loader.pushProjection(emptyFieldList);
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


}

// vim: et sw=4 sts=4
