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

package org.apache.synapse.config.xml;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.mediators.v2.LoggerMediator;

import java.util.Properties;
import javax.xml.namespace.QName;

/**
 * Creates a Logger mediator that logs messages using commons-logging.
 *
 * <pre>
 * &lt;logger [level="info|debug|trace|warn|error|fatal"] message="string"&gt;
 *      &lt;variables&gt; *
 * &lt;/logger&gt;
 * </pre>
 */
public class LoggerMediatorFactory extends AbstractMediatorFactory {

    public static final String LEVEL_INFO = "INFO";
    public static final String LEVEL_TRACE = "TRACE";
    public static final String LEVEL_DEBUG = "DEBUG";
    public static final String LEVEL_WARN = "WARN";
    public static final String LEVEL_ERROR = "ERROR";
    public static final String LEVEL_FATAL = "FATAL";
    private static final QName LOG_Q = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "logger");
    private static final QName ATT_LEVEL = new QName("level");
    private static final QName ATT_MESSAGE = new QName("message");

    public QName getTagQName() {

        return LOG_Q;
    }

    public Mediator createSpecificMediator(OMElement elem, Properties properties) {

        LoggerMediator loggerMediator = new LoggerMediator();

        OMAttribute message = elem.getAttribute(ATT_MESSAGE);
        if (message != null) {
            loggerMediator.setMessageTemplate(message.getAttributeValue());
        }
        // Set the log level (i.e. INFO, DEBUG, etc..)
        OMAttribute levelAttr = elem.getAttribute(ATT_LEVEL);
        if (levelAttr != null) {
            String level = levelAttr.getAttributeValue().trim().toUpperCase();
            if (LEVEL_INFO.equals(level)) {
                loggerMediator.setLogLevel(LoggerMediator.LOG_LEVEL_INFO);
            } else if (LEVEL_TRACE.equals(level)) {
                loggerMediator.setLogLevel(LoggerMediator.LOG_LEVEL_TRACE);
            } else if (LEVEL_DEBUG.equals(level)) {
                loggerMediator.setLogLevel(LoggerMediator.LOG_LEVEL_DEBUG);
            } else if (LEVEL_WARN.equals(level)) {
                loggerMediator.setLogLevel(LoggerMediator.LOG_LEVEL_WARN);
            } else if (LEVEL_ERROR.equals(level)) {
                loggerMediator.setLogLevel(LoggerMediator.LOG_LEVEL_ERROR);
            } else if (LEVEL_FATAL.equals(level)) {
                loggerMediator.setLogLevel(LoggerMediator.LOG_LEVEL_FATAL);
            } else {
                handleException("Invalid log level. Log level has to be one of " +
                        "the following : INFO, TRACE, DEBUG, WARN, ERROR, FATAL");
            }
        }
        loggerMediator.addAllProperties(MediatorVariableFactory.getMediatorProperties(elem));
        addAllCommentChildrenToList(elem, loggerMediator.getCommentsList());
        return loggerMediator;
    }
}
