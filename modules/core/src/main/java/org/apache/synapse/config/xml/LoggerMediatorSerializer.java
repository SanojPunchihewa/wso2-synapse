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

import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.mediators.v2.LoggerMediator;

import static org.apache.synapse.config.xml.LoggerMediatorFactory.LEVEL_TRACE;

/**
 * <pre>
 * &lt;logger [level="info|debug|trace|warn|error|fatal"] message="string"&gt;
 *      &lt;variables&gt; *
 * &lt;/logger&gt;
 * </pre>
 */
public class LoggerMediatorSerializer extends AbstractMediatorSerializer {

    public OMElement serializeSpecificMediator(Mediator m) {

        if (!(m instanceof LoggerMediator)) {
            handleException("Unsupported mediator passed in for serialization : " + m.getType());
        }

        LoggerMediator mediator = (LoggerMediator) m;
        OMElement logElement = fac.createOMElement("logger", synNS);
        saveTracingState(logElement, mediator);

        if (mediator.getMessageTemplate() != null) {
            logElement.addAttribute(fac.createOMAttribute("message", nullNS, mediator.getMessageTemplate()));
        }

        int logLevel = mediator.getLogLevel();

        if (logLevel != LoggerMediator.LOG_LEVEL_INFO) {
            switch (logLevel) {
                case LoggerMediator.LOG_LEVEL_TRACE:
                    logElement.addAttribute(fac.createOMAttribute(
                            "level", nullNS, LEVEL_TRACE));
                    break;
                case LoggerMediator.LOG_LEVEL_DEBUG:
                    logElement.addAttribute(fac.createOMAttribute(
                            "level", nullNS, LoggerMediatorFactory.LEVEL_DEBUG));
                    break;
                case LoggerMediator.LOG_LEVEL_WARN:
                    logElement.addAttribute(fac.createOMAttribute(
                            "level", nullNS, LoggerMediatorFactory.LEVEL_WARN));
                    break;
                case LoggerMediator.LOG_LEVEL_ERROR:
                    logElement.addAttribute(fac.createOMAttribute(
                            "level", nullNS, LoggerMediatorFactory.LEVEL_ERROR));
                    break;
                case LoggerMediator.LOG_LEVEL_FATAL:
                    logElement.addAttribute(fac.createOMAttribute(
                            "level", nullNS, LoggerMediatorFactory.LEVEL_FATAL));
                    break;
            }
        }
        super.serializeVariables(logElement, mediator.getVariables());
        serializeComments(logElement, mediator.getCommentsList());
        return logElement;
    }

    public String getMediatorClassName() {

        return LoggerMediator.class.getName();
    }
}
