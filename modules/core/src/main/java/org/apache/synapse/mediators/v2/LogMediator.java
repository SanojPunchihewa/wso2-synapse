package org.apache.synapse.mediators.v2;

import org.apache.logging.log4j.ThreadContext;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.commons.CorrelationConstants;
import org.apache.synapse.mediators.MediatorProperty;
import org.apache.synapse.mediators.transform.pfutils.FreeMarkerTemplateProcessor;

import java.util.ArrayList;
import java.util.List;

public class LogMediator extends org.apache.synapse.mediators.builtin.LogMediator {

    public static final int LOG_LEVEL_INFO = 0;
    public static final int LOG_LEVEL_DEBUG = 1;
    public static final int LOG_LEVEL_TRACE = 2;
    public static final int LOG_LEVEL_WARN = 3;
    public static final int LOG_LEVEL_ERROR = 4;
    public static final int LOG_LEVEL_FATAL = 5;
    private int logLevel = LOG_LEVEL_INFO;
    private FreeMarkerTemplateProcessor freeMarkerTemplateProcessor;
    private final List<MediatorProperty> properties = new ArrayList<>();
    private boolean isExpression = false;

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
            synLog.traceOrDebug("Start : Log mediator");

            if (synLog.isTraceTraceEnabled()) {
                synLog.traceTrace("Message : " + synCtx.getEnvelope());
            }
        }

        switch (logLevel) {
            case LOG_LEVEL_INFO:
                synLog.auditLog(freeMarkerTemplateProcessor.processTemplate(synCtx));
                break;
            case LOG_LEVEL_TRACE:
                if (synLog.isTraceEnabled()) {
                    synLog.auditTrace(freeMarkerTemplateProcessor.processTemplate(synCtx));
                }
                break;
            case LOG_LEVEL_DEBUG:
                if (synLog.isDebugEnabled()) {
                    synLog.auditDebug(freeMarkerTemplateProcessor.processTemplate(synCtx));
                }
                break;
            case LOG_LEVEL_WARN:
                synLog.auditWarn(freeMarkerTemplateProcessor.processTemplate(synCtx));
                break;
            case LOG_LEVEL_ERROR:
                synLog.auditError(freeMarkerTemplateProcessor.processTemplate(synCtx));
                break;
            case LOG_LEVEL_FATAL:
                synLog.auditFatal(freeMarkerTemplateProcessor.processTemplate(synCtx));
                break;
        }
        synLog.traceOrDebug("End : Log mediator");
        return true;
    }

    public void setTemplateProcessor(FreeMarkerTemplateProcessor freeMarkerTemplateProcessor) {
        this.freeMarkerTemplateProcessor = freeMarkerTemplateProcessor;
    }
}
