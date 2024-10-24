/*
 *  Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.synapse.config.xml;

public class LoggerMediatorSerializationTest extends AbstractTestCase {

    LoggerMediatorFactory loggerMediatorFactory;
    LoggerMediatorSerializer loggerMediatorSerializer;

    public LoggerMediatorSerializationTest() {

        super(LoggerMediatorSerializationTest.class.getName());
        loggerMediatorFactory = new LoggerMediatorFactory();
        loggerMediatorSerializer = new LoggerMediatorSerializer();
    }

    public void testLoggerMediatorSerializationWithMessage() throws Exception {

        assertTrue(serialization(getXMLOfLoggerWithMessage("TRACE", "Test message"), loggerMediatorFactory,
                loggerMediatorSerializer));
        assertTrue(serialization(getXMLOfLoggerWithMessage("DEBUG", "Process message with #[var.messageID]"),
                loggerMediatorFactory, loggerMediatorSerializer));
        assertTrue(serialization(getXMLOfLoggerWithMessage("WARN", "Process message with #[payload.messageID]"),
                loggerMediatorFactory, loggerMediatorSerializer));
        assertTrue(serialization(getXMLOfLoggerWithMessage("ERROR", "Test message"), loggerMediatorFactory,
                loggerMediatorSerializer));
        assertTrue(serialization(getXMLOfLoggerWithMessage("FATAL", "Test message"), loggerMediatorFactory,
                loggerMediatorSerializer));
    }

    public void testLoggerMediatorSerializationWithAdditionalParams() throws Exception {

        assertTrue(serialization(getXMLOfLoggerWithAdditionalParams(), loggerMediatorFactory, loggerMediatorSerializer));
    }

    private String getXMLOfLoggerWithMessage(String level, String message) {

        return "<logger xmlns=\"http://ws.apache.org/ns/synapse\" message=\"" + message + "\" level=\"" + level + "\"></logger>";

    }

    private String getXMLOfLoggerWithAdditionalParams() {

        return "<logger xmlns=\"http://ws.apache.org/ns/synapse\" " +
                "message=\"Processing message with #[var.var1]\">" +
                "<variable name=\"pets\" expression=\"json-eval($.result.pets)\"/>" +
                "<variable name=\"custom-id\" value=\"abcd-123adb\"/>" +
                "</logger>";

    }
}
