/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.synapse.samples.framework.tests.mediation;

import org.apache.http.HttpStatus;
import org.apache.synapse.samples.framework.SynapseTestCase;
import org.apache.synapse.samples.framework.clients.BasicHttpClient;
import org.apache.synapse.samples.framework.clients.HttpResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Test case for Sample 20: Test continue iterations on failure for ForEach Mediator
 */
public class Sample20 extends SynapseTestCase {

    public Sample20() {

        super(20);
    }

    public void testForEachWithContinue() throws Exception {

        String jsonPayload = "{  \n" +
                "   \"info\":[  \n" +
                "      {  \n" +
                "         \"id\":\"IDABC1\",\n" +
                "         \"classid\":1,\n" +
                "         \"name\":\"ABC\"\n" +
                "      },\n" +
                "      {  \n" +
                "         \"id\":\"IDEFG2\",\n" +
                "         \"classid\":2,\n" +
                "         \"name\":\"EFG\"\n" +
                "      },\n" +
                "      {  \n" +
                "         \"id\":\"IDHIJ3\",\n" +
                "         \"classid\":3,\n" +
                "         \"name\":\"HIJ\"\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        String xmlPayload = "<root>\n" +
                "\t<info>\n" +
                "\t\t<id>IDABC1</id>\n" +
                "\t\t<classid>1</classid>\n" +
                "\t\t<name>ABC</name>\n" +
                "\t</info>\n" +
                "\t<info>\n" +
                "\t\t<id>IDEFG2</id>\n" +
                "\t\t<classid>2</classid>\n" +
                "\t\t<name>EFG</name>\n" +
                "\t</info>\n" +
                "\t<info>\n" +
                "\t\t<id>IDHIJ3</id>\n" +
                "\t\t<classid>3</classid>\n" +
                "\t\t<name>HIJ</name>\n" +
                "\t</info>\n" +
                "</root>";

//        Map<String,String> headers = new HashMap<String, String>();
//        headers.put("Content-Type", "application/json");

        BasicHttpClient client = new BasicHttpClient();
        HttpResponse response = client.doPost("http://127.0.0.1:8280/foreach-test/xml", xmlPayload.getBytes(),
                "application/xml");
        assertEquals(HttpStatus.SC_OK, response.getStatus());
    }
}

