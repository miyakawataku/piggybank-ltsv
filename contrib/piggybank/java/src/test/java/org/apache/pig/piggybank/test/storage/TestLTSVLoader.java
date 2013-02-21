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

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.test.Util;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
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
        assertTuple(it.next(), map("id", byteArray("001"), "name", byteArray("John")));
        assertTuple(it.next(), map("id", byteArray("002"), "name", byteArray("Paul")));
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
        assertTuple(it.next(), byteArray("host1.example.org"), byteArray("Opera/9.80"));
        assertTuple(it.next(), byteArray("host1.example.org"), byteArray("Opera/9.80"));
        assertTuple(it.next(), byteArray("pc.example.com"), byteArray("Mozilla/5.0"));
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
        assertTuple(it.next(), 1, "John");
        assertTuple(it.next(), 2, "Paul");
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
        assertTuple(it.next(), "host1.example.org", "Opera/9.80");
        assertTuple(it.next(), "host1.example.org", "Opera/9.80");
        assertTuple(it.next(), "pc.example.com", "Mozilla/5.0");
        assertThat(it.hasNext(), is(false));
    }


    /** Malformed columns are not contained in the output. */
    @Test
    public void test_malformed_column_is_skipped() throws Exception {
        String inputFileName = "TestLTSVLoader-test_malformed_column_is_skipped.ltsv";
        Util.createLocalInputFile(inputFileName, new String[] {
            "\t\tid:001\terrordata1\tname:John\t\t",
            "\t\tid:002\terrordata2\tname:Paul\t\t"
        });
        pigServer.registerQuery(String.format(
                    "beatle = LOAD '%s' USING org.apache.pig.piggybank.storage.LTSVLoader();"
                    , inputFileName));
        Iterator<Tuple> it = pigServer.openIterator("beatle");
        assertTuple(it.next(), map("id", byteArray("001"), "name", byteArray("John")));
        assertTuple(it.next(), map("id", byteArray("002"), "name", byteArray("Paul")));
        assertThat(it.hasNext(), is(false));
    }


    /**
     * Asserts that the actual tuple is a tuple of the given elements,
     */
    private void assertTuple(Tuple actual, Object ... expectedElements) throws Exception {
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


}

// vim: et sw=4 sts=4
