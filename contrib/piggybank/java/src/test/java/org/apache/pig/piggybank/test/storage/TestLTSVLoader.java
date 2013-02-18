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
import org.apache.pig.test.MiniCluster;
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


    /** Testing server. */
    private final PigServer pigServer;


    public TestLTSVLoader() throws Exception {
        this.pigServer = new PigServer(ExecType.LOCAL, new Properties());
        this.pigServer.getPigContext().getProperties().setProperty("mapred.map.max.attempts", "1");
        this.pigServer.getPigContext().getProperties().setProperty("mapred.reduce.max.attempts", "1");
    }


    @Test
    public void test_extract_map_without_projection() throws Exception {
        String inputFileName = "TestLTSVLoader-test_stub.ltsv";
        Util.createLocalInputFile(inputFileName, new String[] {
            "id:001\tname:John",
            "id:002\tname:Paul"
        });
        String script = String.format(
                "beatle = LOAD '%s' USING org.apache.pig.piggybank.storage.LTSVLoader();"
                , inputFileName);
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("beatle");
        assertUnaryTuple(it.next(), map("id", byteArray("001"), "name", byteArray("John")));
        assertUnaryTuple(it.next(), map("id", byteArray("002"), "name", byteArray("Paul")));
    }


    /**
     * Asserts that the actual tuple is an unary tuple,
     * and the only element is equal to {@code expectedElement}.
     */
    private void assertUnaryTuple(Tuple actual, Object expectedElement) throws Exception {
        assertThat(actual.size(), is(1));
        assertThat(actual.get(0), is(expectedElement));
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
