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

package org.apache.synapse.mediators.v2;

import org.apache.logging.log4j.ThreadContext;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.commons.CorrelationConstants;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.MediatorVariable;
import org.apache.synapse.util.ExpressionTemplateProcessor;

import java.util.ArrayList;
import java.util.List;

public class LoggerMediator extends AbstractMediator {

    public static final int LOG_LEVEL_INFO = 0;
    public static final int LOG_LEVEL_DEBUG = 1;
    public static final int LOG_LEVEL_TRACE = 2;
    public static final int LOG_LEVEL_WARN = 3;
    public static final int LOG_LEVEL_ERROR = 4;
    public static final int LOG_LEVEL_FATAL = 5;
    private final List<MediatorVariable> variables = new ArrayList<>();
    private int logLevel = LOG_LEVEL_INFO;
    private String messageTemplate;

    public boolean mediate(MessageContext synCtx) {

        Object correlationId = getCorrelationId(synCtx);
        if (correlationId instanceof String) {
            ThreadContext.put(CorrelationConstants.CORRELATION_MDC_PROPERTY, String.valueOf(correlationId));
        }

        if (synCtx.getEnvironment().isDebuggerEnabled()) {
            if (super.divertMediationRoute(synCtx)) {
                return true;
            }
        }

        SynapseLog synLog = getLog(synCtx);

        if (synLog.isTraceOrDebugEnabled()) {
            synLog.traceOrDebug("Start : Logger mediator");

            if (synLog.isTraceTraceEnabled()) {
                synLog.traceTrace("Message : " + synCtx.getEnvelope());
            }
        }

        switch (logLevel) {
            case LOG_LEVEL_INFO:
                synLog.auditLog(generateLogMessage(synCtx));
                break;
            case LOG_LEVEL_TRACE:
                if (synLog.isTraceEnabled()) {
                    synLog.auditTrace(generateLogMessage(synCtx));
                }
                break;
            case LOG_LEVEL_DEBUG:
                if (synLog.isDebugEnabled()) {
                    synLog.auditDebug(generateLogMessage(synCtx));
                }
                break;
            case LOG_LEVEL_WARN:
                synLog.auditWarn(generateLogMessage(synCtx));
                break;
            case LOG_LEVEL_ERROR:
                synLog.auditError(generateLogMessage(synCtx));
                break;
            case LOG_LEVEL_FATAL:
                synLog.auditFatal(generateLogMessage(synCtx));
                break;
        }
        synLog.traceOrDebug("End : Logger mediator");
        return true;
    }

    private String generateLogMessage(MessageContext synCtx) {

        StringBuffer logMessage = new StringBuffer();
        logMessage.append(processMessageTemplate(synCtx));
        setAdditionalVariables(logMessage, synCtx);
        return logMessage.toString();
    }

    private String processMessageTemplate(MessageContext synCtx) {

        return ExpressionTemplateProcessor.processMessageTemplate(synCtx, messageTemplate);
    }

    private void setAdditionalVariables(StringBuffer sb, MessageContext synCtx) {

        if (!variables.isEmpty()) {
            for (MediatorVariable variable : variables) {
                sb.append(",").append(variable.getName()).append(" = ").append(
                        variable.getValue() != null ? variable.getValue() : variable.getEvaluatedExpression(synCtx));
            }
        }
    }

    public String getMessageTemplate() {

        return messageTemplate;
    }

    public void setMessageTemplate(String messageTemplate) {

        this.messageTemplate = messageTemplate;
    }

    protected Object getCorrelationId(MessageContext synCtx) {

        Object correlationId = null;
        if (synCtx instanceof Axis2MessageContext) {
            Axis2MessageContext axis2Ctx = ((Axis2MessageContext) synCtx);
            correlationId = axis2Ctx.getAxis2MessageContext().getProperty(CorrelationConstants.CORRELATION_ID);
        }
        return correlationId;
    }

    public int getLogLevel() {

        return logLevel;
    }

    public void setLogLevel(int logLevel) {

        this.logLevel = logLevel;
    }

    public List<MediatorVariable> getVariables() {

        return variables;
    }

    public void addAllProperties(List<MediatorVariable> list) {

        variables.addAll(list);
    }
}
