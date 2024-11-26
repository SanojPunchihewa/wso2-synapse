/*
 *  Copyright (c) 2024, WSO2 LLC. (https://www.wso2.com) All Rights Reserved.
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

import com.google.gson.*;
import com.jayway.jsonpath.*;
import java.io.*;
import java.util.*;
import javax.xml.stream.*;
import org.apache.axiom.om.*;
import org.apache.axiom.soap.*;
import org.apache.axis2.Constants;
import org.apache.axis2.*;
import org.apache.axis2.context.*;
import org.apache.http.protocol.*;
import org.apache.synapse.MessageContext;
import org.apache.synapse.*;
import org.apache.synapse.aspects.*;
import org.apache.synapse.aspects.flow.statistics.*;
import org.apache.synapse.aspects.flow.statistics.collectors.*;
import org.apache.synapse.aspects.flow.statistics.data.artifact.*;
import org.apache.synapse.aspects.flow.statistics.util.*;
import org.apache.synapse.commons.json.*;
import org.apache.synapse.config.xml.*;
import org.apache.synapse.continuation.*;
import org.apache.synapse.core.*;
import org.apache.synapse.core.axis2.*;
import org.apache.synapse.endpoints.auth.oauth.*;
import org.apache.synapse.mediators.*;
import org.apache.synapse.mediators.base.*;
import org.apache.synapse.mediators.eip.*;
import org.apache.synapse.mediators.eip.aggregator.*;
import org.apache.synapse.transport.passthru.util.*;
import org.apache.synapse.util.*;

public class ForEachMediator extends AbstractMediator implements ManagedLifecycle, FlowContinuableMediator {

    public static final String JSON_TYPE = "JSON";
    public static final String XML_TYPE = "XML";
    private static final String FOR_EACH_ORIGINAL_MSG_PREFIX = "FOR_EACH_";
    private static final String FOR_EACH_ORIGINAL_MESSAGE_ID = "FOR_EACH_ORIGINAL_MESSAGE_ID";
    private final Object lock = new Object();
    private final Map<String, ForLoopAggregate> activeAggregates = Collections.synchronizedMap(new HashMap<>());
    private final String id;
    private SynapsePath collectionExpression = null;
    private Target target;
    private long completionTimeoutMillis = 0;
    private boolean parallelExecution = true;
    private Integer statisticReportingIndex;
    private String contentType;
    private String resultTarget = null;
    private SynapseEnvironment synapseEnv;

    public ForEachMediator() {

        id = String.valueOf(new Random().nextLong());
    }

    private static void addChildren(List list, OMElement element) {

        for (Object item : list) {
            if (item instanceof OMElement) {
                element.addChild((OMElement) item);
            }
        }
    }

    private static List getMatchingElements(MessageContext messageContext, SynapsePath expression) {

        Object o = expression.objectValueOf(messageContext);
        if (o instanceof OMNode) {
            List list = new ArrayList();
            list.add(o);
            return list;
        } else if (o instanceof List) {
            return (List) o;
        } else {
            return new ArrayList();
        }
    }

    private static void enrichEnvelope(MessageContext messageContext, SynapsePath expression) {

        OMElement enrichingElement;
        List elementList = getMatchingElements(messageContext, expression);
        if (EIPUtils.checkNotEmpty(elementList)) {
            // attach at parent of the first result from the XPath, or to the SOAPBody
            Object o = elementList.get(0);
            if (o instanceof OMElement &&
                    ((OMElement) o).getParent() != null &&
                    ((OMElement) o).getParent() instanceof OMElement) {
                enrichingElement = (OMElement) ((OMElement) o).getParent();
                OMElement body = messageContext.getEnvelope().getBody();
                if (!EIPUtils.isBody(body, enrichingElement)) {
                    OMElement nonBodyElem = enrichingElement;
                    enrichingElement = messageContext.getEnvelope().getBody();
                    addChildren(elementList, enrichingElement);
                    while (!EIPUtils.isBody(body, (OMElement) nonBodyElem.getParent())) {
                        nonBodyElem = (OMElement) nonBodyElem.getParent();
                    }
                    nonBodyElem.detach();
                }
            }
        }
    }

    /**
     * Check whether the message is a scatter message or not
     *
     * @param synCtx MessageContext
     * @return true if the message is a scatter message
     */
    private static boolean isContinuationTriggeredFromMediatorWorker(MessageContext synCtx) {

        Boolean isContinuationTriggeredMediatorWorker =
                (Boolean) synCtx.getProperty(SynapseConstants.CONTINUE_FLOW_TRIGGERED_FROM_MEDIATOR_WORKER);
        return isContinuationTriggeredMediatorWorker != null && isContinuationTriggeredMediatorWorker;
    }

    @Override
    public boolean mediate(MessageContext synCtx) {

        boolean aggregationResult = false;

        SynapseLog synLog = getLog(synCtx);

        if (synLog.isTraceOrDebugEnabled()) {
            synLog.traceOrDebug("Start : For Loop mediator");

            if (synLog.isTraceTraceEnabled()) {
                synLog.traceTrace("Message : " + synCtx.getEnvelope());
            }
        }

        try {
            // Clone the original MessageContext and save it to continue the flow
            MessageContext clonedMessageContext = MessageHelper.cloneMessageContext(synCtx);
            String messageId = FOR_EACH_ORIGINAL_MSG_PREFIX + synCtx.getMessageID();
            MessageCache.getInstance().addMessageContext(messageId, clonedMessageContext);
            synCtx.setProperty(FOR_EACH_ORIGINAL_MESSAGE_ID, messageId);
            synCtx.setProperty(id != null ? EIPConstants.EIP_SHARED_DATA_HOLDER + "." + id :
                    EIPConstants.EIP_SHARED_DATA_HOLDER, new SharedDataHolder());

            Object collection = collectionExpression.objectValueOf(synCtx);

            if (collection instanceof JsonArray) {
                int msgNumber = 0;
                JsonArray list = (JsonArray) collection;
                if (list.isEmpty()) {
                    log.info("No elements found for the expression : " + collectionExpression);
                    return true;
                }
                int msgCount = list.size();
                for (Object item : list) {
                    MessageContext iteratedMsgCtx = getIteratedMessage(synCtx, msgNumber++, msgCount, item);
                    ContinuationStackManager.addReliantContinuationState(iteratedMsgCtx, 0, getMediatorPosition());
                    boolean result = target.mediate(iteratedMsgCtx);
                    if (!parallelExecution && result) {
                        aggregationResult = aggregateMessages(iteratedMsgCtx, synLog);
                    }
                }
            } else if (collection instanceof List) {
                int msgNumber = 0;
                List list = (List) collection;
                if (list.isEmpty()) {
                    log.info("No elements found for the expression : " + collectionExpression);
                    return true;
                }
                int msgCount = list.size();
                for (Object item : list) {
                    MessageContext iteratedMsgCtx = getIteratedMessage(synCtx, msgNumber++, msgCount, item);
                    ContinuationStackManager.addReliantContinuationState(iteratedMsgCtx, 0, getMediatorPosition());
                    boolean result = target.mediate(iteratedMsgCtx);
                    if (!parallelExecution && result) {
                        aggregationResult = aggregateMessages(iteratedMsgCtx, synLog);
                    }
                }
            } else {
                handleException("Expression " + collectionExpression + " did not resolve to a valid array", synCtx);
            }
        } catch (Exception e) {
            handleException("Error executing For Loop mediator", e, synCtx);
        }

        OperationContext opCtx
                = ((Axis2MessageContext) synCtx).getAxis2MessageContext().getOperationContext();
        if (opCtx != null) {
            opCtx.setProperty(Constants.RESPONSE_WRITTEN, "SKIP");
        }
        synCtx.setProperty(StatisticsConstants.CONTINUE_STATISTICS_FLOW, true);
        return aggregationResult;
    }

    private MessageContext getIteratedMessage(MessageContext synCtx, int msgNumber, int msgCount, Object node) throws AxisFault {

        MessageContext newCtx = MessageHelper.cloneMessageContext(synCtx, false, false);
        // Adding an empty envelope since JsonUtil.getNewJsonPayload requires an envelope
        SOAPEnvelope newEnvelope = createNewSoapEnvelope(synCtx.getEnvelope());
        newCtx.setEnvelope(newEnvelope);
        if (node instanceof OMNode) {
            if (newEnvelope.getBody() != null) {
                newEnvelope.getBody().addChild((OMNode) node);
            }
        } else {
            JsonUtil.getNewJsonPayload(((Axis2MessageContext) newCtx).getAxis2MessageContext(), node.toString(), true,
                    true);
        }
        newCtx.setProperty(EIPConstants.AGGREGATE_CORRELATION + "." + id, synCtx.getMessageID());
        newCtx.setProperty(EIPConstants.MESSAGE_SEQUENCE + "." + id, msgNumber + EIPConstants.MESSAGE_SEQUENCE_DELEMITER + msgCount);
        // Set the SCATTER_MESSAGES property to the cloned message context which will be used by the MediatorWorker
        // to continue the mediation from the continuation state
        newCtx.setProperty(SynapseConstants.SCATTER_MESSAGES, true);
        ((Axis2MessageContext) newCtx).getAxis2MessageContext().setServerSide(
                ((Axis2MessageContext) synCtx).getAxis2MessageContext().isServerSide());
        return newCtx;
    }
//
//    private MessageContext getIteratedMessage(MessageContext synCtx, int msgNumber, int msgCount, OMNode omNode) throws AxisFault {
//
//        // clone the message context without cloning the SOAP envelope, for the mediation in iteration.
//        MessageContext newCtx = MessageHelper.cloneMessageContext(synCtx, false, false);
//
//        SOAPEnvelope newEnvelope = createNewSoapEnvelope(synCtx.getEnvelope());
//
//        newCtx.setEnvelope(newEnvelope);
//
//        newCtx.setProperty(EIPConstants.AGGREGATE_CORRELATION + "." + id, synCtx.getMessageID());
//        newCtx.setProperty(EIPConstants.MESSAGE_SEQUENCE + "." + id, msgNumber +
//                EIPConstants.MESSAGE_SEQUENCE_DELEMITER + msgCount);
//        // Set the SCATTER_MESSAGES property to the cloned message context which will be used by the MediatorWorker
//        // to continue the mediation from the continuation state
//        newCtx.setProperty(SynapseConstants.SCATTER_MESSAGES, true);
//        // Set isServerSide property in the cloned message context
//        ((Axis2MessageContext) newCtx).getAxis2MessageContext().setServerSide(
//                ((Axis2MessageContext) synCtx).getAxis2MessageContext().isServerSide());
//        return newCtx;
//    }

    private SOAPEnvelope createNewSoapEnvelope(SOAPEnvelope envelope) {
        SOAPFactory fac;
        if (SOAP11Constants.SOAP_ENVELOPE_NAMESPACE_URI.equals(envelope.getBody().getNamespace().getNamespaceURI())) {
            fac = OMAbstractFactory.getSOAP11Factory();
        } else {
            fac = OMAbstractFactory.getSOAP12Factory();
        }
        return fac.getDefaultEnvelope();
    }

    public void init(SynapseEnvironment synapseEnv) {

        this.synapseEnv = synapseEnv;
        ManagedLifecycle seq = target.getSequence();
        if (seq != null) {
            seq.init(synapseEnv);
        }
        // Registering the mediator for enabling continuation
        synapseEnv.updateCallMediatorCount(true);
    }

    public void destroy() {

        ManagedLifecycle seq = target.getSequence();
        if (seq != null) {
            seq.destroy();
        }
        // Unregistering the mediator for continuation
        synapseEnv.updateCallMediatorCount(false);
    }

    public Target getTarget() {

        return target;
    }

    public void setTarget(Target target) {

        this.target = target;
    }

    @Override
    public boolean mediate(MessageContext synCtx, ContinuationState continuationState) {

        SynapseLog synLog = getLog(synCtx);

        if (synLog.isTraceOrDebugEnabled()) {
            synLog.traceOrDebug("Scatter Gather mediator : Mediating from ContinuationState");
        }

        boolean result;
        // If the continuation is triggered from a mediator worker and has children, then mediate through the sub branch
        // otherwise start aggregation
        if (isContinuationTriggeredFromMediatorWorker(synCtx)) {
            if (continuationState.hasChild()) {
                SequenceMediator branchSequence = target.getSequence();
                boolean isStatisticsEnabled = RuntimeStatisticCollector.isStatisticsEnabled();
                FlowContinuableMediator mediator =
                        (FlowContinuableMediator) branchSequence.getChild(continuationState.getChildContState().getPosition());

                result = mediator.mediate(synCtx, continuationState.getChildContState());
                if (isStatisticsEnabled) {
                    ((Mediator) mediator).reportCloseStatistics(synCtx, null);
                }
            } else {
                result = true;
            }
        } else {
            // If the continuation is triggered from a callback, continue the mediation from the continuation state
            SequenceMediator branchSequence = target.getSequence();
            boolean isStatisticsEnabled = RuntimeStatisticCollector.isStatisticsEnabled();
            if (!continuationState.hasChild()) {
                result = branchSequence.mediate(synCtx, continuationState.getPosition() + 1);
            } else {
                FlowContinuableMediator mediator =
                        (FlowContinuableMediator) branchSequence.getChild(continuationState.getPosition());

                result = mediator.mediate(synCtx, continuationState.getChildContState());
                if (isStatisticsEnabled) {
                    ((Mediator) mediator).reportCloseStatistics(synCtx, null);
                }
            }
            // If the mediation is completed, remove the child continuation state from the stack, so the aggregation
            // will continue the mediation from the parent continuation state
            ContinuationStackManager.removeReliantContinuationState(synCtx);
        }
        if (result) {
            return aggregateMessages(synCtx, synLog);
        }
        return false;
    }

    private boolean aggregateMessages(MessageContext synCtx, SynapseLog synLog) {

        ForLoopAggregate aggregate = null;
        String correlationIdName = (id != null ? EIPConstants.AGGREGATE_CORRELATION + "." + id :
                EIPConstants.AGGREGATE_CORRELATION);

        Object correlationID = synCtx.getProperty(correlationIdName);
        String correlation;

        // When the target sequences are not content aware, the message builder won't get triggered.
        // Therefore, we need to build the message to do the aggregation.
        try {
            RelayUtils.buildMessage(((Axis2MessageContext) synCtx).getAxis2MessageContext());
        } catch (IOException | XMLStreamException e) {
            handleException("Error building the message", e, synCtx);
        }
        if (correlationID instanceof String) {
            correlation = (String) correlationID;
            while (aggregate == null) {
                synchronized (lock) {
                    if (activeAggregates.containsKey(correlation)) {
                        aggregate = activeAggregates.get(correlation);
                        if (aggregate != null) {
                            if (!aggregate.getLock()) {
                                aggregate = null;
                            }
                        } else {
                            break;
                        }
                    } else {
                        if (synLog.isTraceOrDebugEnabled()) {
                            synLog.traceOrDebug("Creating new Aggregator - " +
                                    (completionTimeoutMillis > 0 ? "expires in : "
                                            + (completionTimeoutMillis / 1000) + "secs" :
                                            "without expiry time"));
                        }
                        if (isAggregationCompleted(synCtx)) {
                            return false;
                        }
                        aggregate = new ForLoopAggregate(synCtx.getEnvironment(), correlation, completionTimeoutMillis, this,
                                synCtx.getFaultStack().peek());

                        if (completionTimeoutMillis > 0) {
                            synchronized (aggregate) {
                                if (!aggregate.isCompleted()) {
                                    try {
                                        synCtx.getConfiguration().getSynapseTimer().
                                                schedule(aggregate, completionTimeoutMillis);
                                    } catch (IllegalStateException e) {
                                        log.warn("Synapse timer already cancelled. Resetting Synapse timer");
                                        synCtx.getConfiguration().setSynapseTimer(new Timer(true));
                                        synCtx.getConfiguration().getSynapseTimer().
                                                schedule(aggregate, completionTimeoutMillis);
                                    }
                                }
                            }
                        }
                        aggregate.getLock();
                        activeAggregates.put(correlation, aggregate);
                    }
                }
            }
        } else {
            synLog.traceOrDebug("Unable to find aggregation correlation property");
            return true;
        }
        // if there is an aggregate continue on aggregation
        if (aggregate != null) {
            boolean collected = aggregate.addMessage(synCtx);
            if (synLog.isTraceOrDebugEnabled()) {
                if (collected) {
                    synLog.traceOrDebug("Collected a message during aggregation");
                    if (synLog.isTraceTraceEnabled()) {
                        synLog.traceTrace("Collected message : " + synCtx);
                    }
                }
            }
            if (aggregate.isComplete(synLog)) {
                synLog.traceOrDebug("Aggregation completed");
                boolean onCompleteSeqResult = completeAggregate(aggregate);
                synLog.traceOrDebug("End : Scatter Gather mediator");
                return onCompleteSeqResult;
            } else {
                aggregate.releaseLock();
            }
        } else {
            synLog.traceOrDebug("Unable to find an aggregate for this message - skip");
            return true;
        }
        return false;
    }

    private boolean isAggregationCompleted(MessageContext synCtx) {

        Object aggregateTimeoutHolderObj =
                synCtx.getProperty(id != null ? EIPConstants.EIP_SHARED_DATA_HOLDER + "." + id :
                        EIPConstants.EIP_SHARED_DATA_HOLDER);

        if (aggregateTimeoutHolderObj != null) {
            SharedDataHolder sharedDataHolder = (SharedDataHolder) aggregateTimeoutHolderObj;
            if (sharedDataHolder.isAggregationCompleted()) {
                if (log.isDebugEnabled()) {
                    log.debug("Received a response for already completed Aggregate");
                }
                return true;
            }
        }
        return false;
    }

    public boolean completeAggregate(ForLoopAggregate aggregate) {

        boolean markedCompletedNow = false;
        boolean wasComplete = aggregate.isCompleted();
        if (wasComplete) {
            return false;
        }

        if (log.isDebugEnabled()) {
            log.debug("Aggregation completed or timed out");
        }

        // cancel the timer
        synchronized (this) {
            if (!aggregate.isCompleted()) {
                aggregate.cancel();
                aggregate.setCompleted(true);

                MessageContext lastMessage = aggregate.getLastMessage();
                if (lastMessage != null) {
                    Object aggregateTimeoutHolderObj =
                            lastMessage.getProperty(id != null ? EIPConstants.EIP_SHARED_DATA_HOLDER + "." + id :
                                    EIPConstants.EIP_SHARED_DATA_HOLDER);

                    if (aggregateTimeoutHolderObj != null) {
                        SharedDataHolder sharedDataHolder = (SharedDataHolder) aggregateTimeoutHolderObj;
                        sharedDataHolder.markAggregationCompletion();
                    }
                }
                markedCompletedNow = true;
            }
        }

        if (!markedCompletedNow) {
            return false;
        }

        MessageContext originalMessageContext = MessageCache.getInstance().removeMessageContext(
                (String) aggregate.getLastMessage().getProperty(FOR_EACH_ORIGINAL_MESSAGE_ID));

        if (updateOriginalContent()) {
            updateOriginalPayload(originalMessageContext, aggregate);
        } else {
            setAggregatedMessageAsVariable(originalMessageContext, aggregate);
        }
        StatisticDataCollectionHelper.collectAggregatedParents(aggregate.getMessages(), originalMessageContext);
        aggregate.clear();
        activeAggregates.remove(aggregate.getCorrelation());
        // Update the continuation state to current mediator position as we are using the original message context
        ContinuationStackManager.updateSeqContinuationState(originalMessageContext, getMediatorPosition());
        SeqContinuationState seqContinuationState = (SeqContinuationState) ContinuationStackManager.peakContinuationStateStack(originalMessageContext);
        boolean result = false;

        // Set CONTINUE_STATISTICS_FLOW to avoid mark event collection as finished before the aggregation is completed
        originalMessageContext.setProperty(StatisticsConstants.CONTINUE_STATISTICS_FLOW, true);
        if (RuntimeStatisticCollector.isStatisticsEnabled()) {
            CloseEventCollector.closeEntryEvent(originalMessageContext, getMediatorName(), ComponentType.MEDIATOR,
                    statisticReportingIndex, isContentAltering());
        }

        if (seqContinuationState != null) {
            SequenceMediator sequenceMediator = ContinuationStackManager.retrieveSequence(originalMessageContext, seqContinuationState);
            result = sequenceMediator.mediate(originalMessageContext, seqContinuationState);
            if (RuntimeStatisticCollector.isStatisticsEnabled()) {
                sequenceMediator.reportCloseStatistics(originalMessageContext, null);
            }
        }
        CloseEventCollector.closeEventsAfterScatterGather(originalMessageContext);
        return result;
    }

    private void setContentTypeHeader(Object resultValue, org.apache.axis2.context.MessageContext axis2MessageCtx) {

        axis2MessageCtx.setProperty(Constants.Configuration.CONTENT_TYPE, resultValue);
        Object o = axis2MessageCtx.getProperty(org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS);
        Map headers = (Map) o;
        if (headers != null) {
            headers.put(HTTP.CONTENT_TYPE, resultValue);
        }
    }

    private void setAggregatedMessageAsVariable(MessageContext originalMessageContext, ForLoopAggregate aggregate) {

        Object variable = originalMessageContext.getVariable(resultTarget);
        for (MessageContext synCtx : aggregate.getMessages()) {
            if (Objects.equals(contentType, JSON_TYPE)) {
                if (variable instanceof JsonObject) {
                    Object prop = synCtx.getProperty(EIPConstants.MESSAGE_SEQUENCE + "." + id);
                    String[] msgSequence = prop.toString().split(EIPConstants.MESSAGE_SEQUENCE_DELEMITER);
                    JsonElement result = (EIPUtils.tryParseJsonString(new JsonParser(),
                            JsonUtil.jsonPayloadToString(((Axis2MessageContext) synCtx).getAxis2MessageContext())));
                    ((JsonElement) variable).getAsJsonObject().add(msgSequence[0], result);
                } else {
                    handleException(aggregate, "Error merging aggregation results to variable : " + resultTarget +
                            " expected a JSON type variable but found " + variable.getClass().getName(), null, synCtx);
                }
            } else {
//                    if (variable instanceof OMElement) {
//                        List list = getMatchingElements(synCtx, aggregationExpression);
//                        addChildren(list, (OMElement) variable);
//                    } else {
//                        handleException(aggregate, "Error merging aggregation results to variable : " + resultTarget +
//                                " expected an OMElement type variable but found " + variable.getClass().getName(), null, synCtx);
//                    }
            }

        }

    }

    private void updateOriginalPayload(MessageContext originalMessageContext, ForLoopAggregate aggregate) {

        Object collection = this.collectionExpression.objectValueOf(originalMessageContext);

        //Read the complete JSON payload from the synCtx
        String jsonPayload = JsonUtil.jsonPayloadToString(((Axis2MessageContext) originalMessageContext).getAxis2MessageContext());
        DocumentContext parsedJsonPayload = JsonPath.parse(jsonPayload);

        if (collection instanceof JsonArray) {
            JsonArray jsonArray = (JsonArray) collection;
            for (MessageContext synCtx : aggregate.getMessages()) {
                if (Objects.equals(contentType, JSON_TYPE)) {
                    Object prop = synCtx.getProperty(EIPConstants.MESSAGE_SEQUENCE + "." + id);
                    String[] msgSequence = prop.toString().split(EIPConstants.MESSAGE_SEQUENCE_DELEMITER);
                    JsonElement result = (EIPUtils.tryParseJsonString(new JsonParser(),
                            JsonUtil.jsonPayloadToString(((Axis2MessageContext) synCtx).getAxis2MessageContext())));
                    jsonArray.set(Integer.parseInt(msgSequence[0]), result);
                } else {
//                    if (variable instanceof OMElement) {
//                        List list = getMatchingElements(synCtx, aggregationExpression);
//                        addChildren(list, (OMElement) variable);
//                    } else {
//                        handleException(aggregate, "Error merging aggregation results to variable : " + resultTarget +
//                                " expected an OMElement type variable but found " + variable.getClass().getName(), null, synCtx);
//                    }
                }
            }
            JsonPath jsonPath = getJsonPathFromExpression(this.collectionExpression.getExpression());
            JsonElement jsonPayloadElement;
            if (isWholeContent(jsonPath)) {
                jsonPayloadElement = jsonArray;
            } else {
                jsonPayloadElement = parsedJsonPayload.set(jsonPath, jsonArray).json();
            }
            if (isCollectionReferencedByVariable(this.collectionExpression)) {
                String variableName = getVariableName(this.collectionExpression);
                originalMessageContext.setVariable(variableName, jsonPayloadElement);
            } else {
                try {
                    JsonUtil.getNewJsonPayload(((Axis2MessageContext) originalMessageContext).getAxis2MessageContext(),
                            jsonPayloadElement.toString(), true, true);
                } catch (AxisFault af) {
                    handleException("Error updating the json stream after foreach transformation", af, originalMessageContext);
                }
            }
        }
    }

    @Override
    public Integer reportOpenStatistics(MessageContext messageContext, boolean isContentAltering) {

        statisticReportingIndex = OpenEventCollector.reportFlowContinuableEvent(messageContext, getMediatorName(),
                ComponentType.MEDIATOR, getAspectConfiguration(), isContentAltering() || isContentAltering);
        return statisticReportingIndex;
    }

    @Override
    public void reportCloseStatistics(MessageContext messageContext, Integer currentIndex) {

        // Do nothing here as the close event is reported in the completeAggregate method
    }

    @Override
    public void setComponentStatisticsId(ArtifactHolder holder) {

        if (getAspectConfiguration() == null) {
            configure(new AspectConfiguration(getMediatorName()));
        }
        String sequenceId =
                StatisticIdentityGenerator.getIdForFlowContinuableMediator(getMediatorName(), ComponentType.MEDIATOR, holder);
        getAspectConfiguration().setUniqueId(sequenceId);
        target.setStatisticIdForMediators(holder);
        StatisticIdentityGenerator.reportingFlowContinuableEndEvent(sequenceId, ComponentType.MEDIATOR, holder);
    }

    @Override
    public boolean isContentAltering() {

        return true;
    }

    private void handleException(ForLoopAggregate aggregate, String msg, Exception exception, MessageContext msgContext) {

        aggregate.clear();
        activeAggregates.clear();
        if (exception != null) {
            super.handleException(msg, exception, msgContext);
        } else {
            super.handleException(msg, msgContext);
        }
    }

    public long getCompletionTimeoutMillis() {

        return completionTimeoutMillis;
    }

    public void setCompletionTimeoutMillis(long completionTimeoutMillis) {

        this.completionTimeoutMillis = completionTimeoutMillis;
    }

    public String getContentType() {

        return contentType;
    }

    public void setContentType(String contentType) {

        this.contentType = contentType;
    }

    public String getResultTarget() {

        return resultTarget;
    }

    public void setResultTarget(String resultTarget) {

        this.resultTarget = resultTarget;
    }

    public SynapsePath getCollectionExpression() {

        return collectionExpression;
    }

    public void setCollectionExpression(SynapsePath collectionExpression) {

        this.collectionExpression = collectionExpression;
    }

    public boolean getParallelExecution() {

        return this.parallelExecution;
    }

    public void setParallelExecution(boolean parallelExecution) {

        this.parallelExecution = parallelExecution;
    }

    private boolean updateOriginalContent() {

        return resultTarget == null;
    }

    public String getId() {
        return id;
    }

    private String getVariableName(SynapsePath expression) {

        return expression.getExpression().split("\\.")[1];
    }

    private boolean isCollectionReferencedByVariable(SynapsePath expression) {

        return expression.getExpression().startsWith("var.");
    }

    private JsonPath getJsonPathFromExpression(String expression) {

        String jsonPath = expression;
        if (jsonPath.startsWith("payload")) {
            jsonPath = jsonPath.replace("payload", "$");
        } else if (jsonPath.startsWith("var.")) {
            // Remove the "var." prefix and variable name and replace it with "$" for JSON path
            jsonPath = expression.replaceAll("var\\.\\w+\\.(\\w+)", "\\$.$1").replaceAll("var\\.\\w+", "\\$");
            ;
        }
        return JsonPath.compile(jsonPath);
    }

    private boolean isWholeContent(JsonPath jsonPath) {

        return "$".equals(jsonPath.getPath().trim()) || "$.".equals(jsonPath.getPath().trim());
    }
}
