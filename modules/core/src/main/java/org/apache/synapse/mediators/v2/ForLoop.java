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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMNode;
import org.apache.axiom.soap.SOAP11Constants;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.OperationContext;
import org.apache.http.protocol.HTTP;
import org.apache.synapse.ContinuationState;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.aspects.AspectConfiguration;
import org.apache.synapse.aspects.ComponentType;
import org.apache.synapse.aspects.flow.statistics.StatisticIdentityGenerator;
import org.apache.synapse.aspects.flow.statistics.collectors.CloseEventCollector;
import org.apache.synapse.aspects.flow.statistics.collectors.OpenEventCollector;
import org.apache.synapse.aspects.flow.statistics.collectors.RuntimeStatisticCollector;
import org.apache.synapse.aspects.flow.statistics.data.artifact.ArtifactHolder;
import org.apache.synapse.aspects.flow.statistics.util.StatisticDataCollectionHelper;
import org.apache.synapse.aspects.flow.statistics.util.StatisticsConstants;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.config.xml.SynapsePath;
import org.apache.synapse.continuation.ContinuationStackManager;
import org.apache.synapse.continuation.SeqContinuationState;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.endpoints.auth.oauth.MessageCache;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.FlowContinuableMediator;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.eip.EIPConstants;
import org.apache.synapse.mediators.eip.EIPUtils;
import org.apache.synapse.mediators.eip.SharedDataHolder;
import org.apache.synapse.mediators.eip.Target;
import org.apache.synapse.mediators.eip.aggregator.Aggregate;
import org.apache.synapse.transport.passthru.util.RelayUtils;
import org.apache.synapse.util.MessageHelper;
import org.apache.synapse.util.xpath.SynapseExpression;
import org.jaxen.JaxenException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Timer;
import javax.xml.stream.XMLStreamException;

import static org.apache.synapse.SynapseConstants.XML_CONTENT_TYPE;
import static org.apache.synapse.transport.passthru.PassThroughConstants.JSON_CONTENT_TYPE;

public class ForLoop extends AbstractMediator implements ManagedLifecycle, FlowContinuableMediator {

    public static final String JSON_TYPE = "JSON";
    public static final String XML_TYPE = "XML";
    private static final String FOR_EACHORIGINAL_MSG_PREFIX = "FOR_EACH";
    private static final String FOR_EACHORIGINAL_MESSAGE_ID = "FOR_EACHORIGINAL_MESSAGE_ID";
    private final Object lock = new Object();
    private final Map<String, Aggregate> activeAggregates = Collections.synchronizedMap(new HashMap<>());
    private String id;
    private SynapsePath collectionToLoop = null;
    private Target target;
    private long completionTimeoutMillis = 0;
    private boolean parallelExecution = true;
    private Integer statisticReportingIndex;
    private String contentType;
    private String resultTarget;
    private SynapseEnvironment synapseEnv;

    public SynapsePath getCollectionToLoop() {

        return collectionToLoop;
    }

    public void setCollectionToLoop(SynapsePath collectionToLoop) {

        this.collectionToLoop = collectionToLoop;
    }

    public ForLoop() {

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

    public boolean getParallelExecution() {

        return this.parallelExecution;
    }

    public void setParallelExecution(boolean parallelExecution) {

        this.parallelExecution = parallelExecution;
    }

    public String getId() {

        return id;
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

            if (!isTargetBody()) {
                // Clone the original MessageContext and save it to continue the flow using it when the scatter gather
                // output is set to a variable
                MessageContext clonedMessageContext = MessageHelper.cloneMessageContext(synCtx);
                String messageId = FOR_EACHORIGINAL_MSG_PREFIX + synCtx.getMessageID();
                MessageCache.getInstance().addMessageContext(messageId, clonedMessageContext);
                synCtx.setProperty(FOR_EACHORIGINAL_MESSAGE_ID, messageId);
            }

            synCtx.setProperty(id != null ? EIPConstants.EIP_SHARED_DATA_HOLDER + "." + id :
                    EIPConstants.EIP_SHARED_DATA_HOLDER, new SharedDataHolder());
//        Iterator<Target> iter = targets.iterator();
//        int i = 0;
//        while (iter.hasNext()) {
//            if (synLog.isTraceOrDebugEnabled()) {
//                synLog.traceOrDebug("Submitting " + (i + 1) + " of " + targets.size() +
//                        " messages for " + (parallelExecution ? "parallel processing" : "sequential processing"));
//            }
//
//            MessageContext clonedMsgCtx = getClonedMessageContext(synCtx, i++, targets.size());
//            ContinuationStackManager.addReliantContinuationState(clonedMsgCtx, i - 1, getMediatorPosition());
//            boolean result = iter.next().mediate(clonedMsgCtx);
//            if (!parallelExecution && result) {
//                aggregationResult = aggregateMessages(clonedMsgCtx, synLog);
//            }
//        }

            Object collection = collectionToLoop.objectValueOf(synCtx);

            if (collection instanceof JsonArray) {
                int msgNumber = 0;
                JsonArray list = (JsonArray) collection;
                int msgCount = list.size();
                for (Object item : list) {
//                if (synLog.isTraceOrDebugEnabled()) {
//                    synLog.traceOrDebug("Submitting " + " of " + targets.size() +
//                            " messages for " + (parallelExecution ? "parallel processing" : "sequential processing"));
//                }
                    MessageContext iteratedMsgCtx = getIteratedMessage(synCtx, msgNumber++, msgCount, item);
                    ContinuationStackManager.addReliantContinuationState(iteratedMsgCtx, 0, getMediatorPosition());
                    boolean result = target.mediate(iteratedMsgCtx);
                    if (!parallelExecution && result) {
                        aggregationResult = aggregateMessages(iteratedMsgCtx, synLog);
                    }
                }
            } else {
                return false;
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

        // clone the message for the mediation in iteration
        MessageContext newCtx = MessageHelper.cloneMessageContext(synCtx, false, false);
        // Adding an empty envelope since JsonUtil.getNewJsonPayload requires an envelope
        SOAPFactory fac;
        if (SOAP11Constants.SOAP_ENVELOPE_NAMESPACE_URI
                .equals(synCtx.getEnvelope().getBody().getNamespace().getNamespaceURI())) {
            fac = OMAbstractFactory.getSOAP11Factory();
        } else {
            fac = OMAbstractFactory.getSOAP12Factory();
        }
        SOAPEnvelope newEnvelope = fac.getDefaultEnvelope();
        newCtx.setEnvelope(newEnvelope);

        // set the parent correlation details to the cloned MC -
        //                              for the use of aggregation like tasks
        newCtx.setProperty(EIPConstants.AGGREGATE_CORRELATION + "." + id, synCtx.getMessageID());
        // set the messageSequence property for possibal aggreagtions
        newCtx.setProperty(EIPConstants.MESSAGE_SEQUENCE + "." + id, msgNumber + EIPConstants.MESSAGE_SEQUENCE_DELEMITER + msgCount);

        // write the new JSON message to the stream
        JsonUtil.getNewJsonPayload(((Axis2MessageContext) newCtx).getAxis2MessageContext(), node.toString(), true,
                true);

        // Set isServerSide property in the cloned message context
        ((Axis2MessageContext) newCtx).getAxis2MessageContext().setServerSide(
                ((Axis2MessageContext) synCtx).getAxis2MessageContext().isServerSide());

        return newCtx;
    }

    public void init(SynapseEnvironment synapseEnv) {

        this.synapseEnv = synapseEnv;
//        for (Target target : targets) {
        ManagedLifecycle seq = target.getSequence();
        if (seq != null) {
            seq.init(synapseEnv);
        }
//        }
        // Registering the mediator for enabling continuation
        synapseEnv.updateCallMediatorCount(true);
    }

    public void destroy() {

//        for (Target target : targets) {
        ManagedLifecycle seq = target.getSequence();
        if (seq != null) {
            seq.destroy();
        }
//        }
        // Unregistering the mediator for continuation
        synapseEnv.updateCallMediatorCount(false);
    }

    /**
     * Clone the provided message context as a new message, and set the aggregation ID and the message sequence count
     *
     * @param synCtx          - MessageContext which is subjected to the cloning
     * @param messageSequence - the position of this message of the cloned set
     * @param messageCount    - total of cloned copies
     * @return MessageContext the cloned message context
     */
    private MessageContext getClonedMessageContext(MessageContext synCtx, int messageSequence, int messageCount) {

        MessageContext newCtx = null;
        try {
            newCtx = MessageHelper.cloneMessageContext(synCtx);
            // Set isServerSide property in the cloned message context
            ((Axis2MessageContext) newCtx).getAxis2MessageContext().setServerSide(
                    ((Axis2MessageContext) synCtx).getAxis2MessageContext().isServerSide());
            // Set the SCATTER_MESSAGES property to the cloned message context which will be used by the MediatorWorker
            // to continue the mediation from the continuation state
            newCtx.setProperty(SynapseConstants.SCATTER_MESSAGES, true);
            if (id != null) {
                newCtx.setProperty(EIPConstants.AGGREGATE_CORRELATION + "." + id, synCtx.getMessageID());
                newCtx.setProperty(EIPConstants.MESSAGE_SEQUENCE + "." + id, messageSequence +
                        EIPConstants.MESSAGE_SEQUENCE_DELEMITER + messageCount);
            } else {
                newCtx.setProperty(EIPConstants.MESSAGE_SEQUENCE, messageSequence +
                        EIPConstants.MESSAGE_SEQUENCE_DELEMITER + messageCount);
            }
        } catch (AxisFault axisFault) {
            handleException("Error cloning the message context", axisFault, synCtx);
        }
        return newCtx;
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
//                int subBranch = ((ReliantContinuationState) continuationState.getChildContState()).getSubBranch();
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
//            int subBranch = ((ReliantContinuationState) continuationState).getSubBranch();

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

        Aggregate aggregate = null;
        String correlationIdName = (id != null ? EIPConstants.AGGREGATE_CORRELATION + "." + id :
                EIPConstants.AGGREGATE_CORRELATION);

        Object correlationID = synCtx.getProperty(correlationIdName);
        String correlation;

        Object result = null;
        // When the target sequences are not content aware, the message builder wont get triggered.
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
                        aggregate = new Aggregate(synCtx.getEnvironment(), correlation, completionTimeoutMillis,
                                -1, -1, this, synCtx.getFaultStack().peek());

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

    public boolean completeAggregate(Aggregate aggregate) {

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

        if (isTargetBody()) {
//            MessageContext newSynCtx = getAggregatedMessage(aggregate);
//
//            if (newSynCtx == null) {
//                log.warn("An aggregation of messages timed out with no aggregated messages", null);
//                return false;
//            }
//            aggregate.clear();
//            activeAggregates.remove(aggregate.getCorrelation());
//
//            // Set content type to the aggregated message
//            setContentType(newSynCtx);
//
//            newSynCtx.setProperty(SynapseConstants.CONTINUE_FLOW_TRIGGERED_FROM_MEDIATOR_WORKER, false);
//            SeqContinuationState seqContinuationState = (SeqContinuationState) ContinuationStackManager.peakContinuationStateStack(newSynCtx);
//            boolean result = false;
//
//            // Set CONTINUE_STATISTICS_FLOW to avoid mark event collection as finished before the aggregation is completed
//            newSynCtx.setProperty(StatisticsConstants.CONTINUE_STATISTICS_FLOW, true);
//            if (RuntimeStatisticCollector.isStatisticsEnabled()) {
//                CloseEventCollector.closeEntryEvent(newSynCtx, getMediatorName(), ComponentType.MEDIATOR,
//                        statisticReportingIndex, isContentAltering());
//            }
//
//            if (seqContinuationState != null) {
//                SequenceMediator sequenceMediator = ContinuationStackManager.retrieveSequence(newSynCtx, seqContinuationState);
//                result = sequenceMediator.mediate(newSynCtx, seqContinuationState);
//                if (RuntimeStatisticCollector.isStatisticsEnabled()) {
//                    sequenceMediator.reportCloseStatistics(newSynCtx, null);
//                }
//            }
//            CloseEventCollector.closeEventsAfterScatterGather(newSynCtx);
//            return result;
        } else {
            MessageContext originalMessageContext = MessageCache.getInstance().removeMessageContext(
                    (String) aggregate.getLastMessage().getProperty(FOR_EACHORIGINAL_MESSAGE_ID));
            setAggregatedMessageAsVariable(originalMessageContext, aggregate);

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
        return false;
    }

    private void setContentTypeHeader(Object resultValue, org.apache.axis2.context.MessageContext axis2MessageCtx) {

        axis2MessageCtx.setProperty(Constants.Configuration.CONTENT_TYPE, resultValue);
        Object o = axis2MessageCtx.getProperty(org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS);
        Map headers = (Map) o;
        if (headers != null) {
            headers.put(HTTP.CONTENT_TYPE, resultValue);
        }
    }

    private void setAggregatedMessageAsVariable(MessageContext originalMessageContext, Aggregate aggregate) {

        Object variable = originalMessageContext.getVariable(resultTarget);
        for (MessageContext synCtx : aggregate.getMessages()) {
            try {
                if (Objects.equals(contentType, JSON_TYPE)) {
                    if (variable instanceof JsonObject) {
                        Object prop = synCtx.getProperty(EIPConstants.MESSAGE_SEQUENCE + "." + getId());
                        String[] msgSequence = prop.toString().split(EIPConstants.MESSAGE_SEQUENCE_DELEMITER);
                        ((JsonElement) variable).getAsJsonObject().add(msgSequence[0], (JsonElement) new SynapseExpression("$").objectValueOf(synCtx));
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
            } catch (SynapseException | JaxenException e) {
//                handleException(aggregate, "Error evaluating expression: " + aggregationExpression.toString(), e, synCtx);
            } catch (JsonSyntaxException e) {
//                handleException(aggregate, "Error reading JSON element: " + aggregationExpression.toString(), e, synCtx);
            }
        }
        StatisticDataCollectionHelper.collectAggregatedParents(aggregate.getMessages(), originalMessageContext);
    }

//    private MessageContext getAggregatedMessage(Aggregate aggregate) {
//
//        MessageContext newCtx = null;
//        JsonArray jsonArray = new JsonArray();
//
//        for (MessageContext synCtx : aggregate.getMessages()) {
//            if (newCtx == null) {
//                try {
//                    newCtx = MessageHelper.cloneMessageContext(synCtx, true, false, true);
//                } catch (AxisFault axisFault) {
//                    handleException(aggregate, "Error creating a copy of the message", axisFault, synCtx);
//                }
//
//                if (log.isDebugEnabled()) {
//                    log.debug("Generating Aggregated message from : " + newCtx.getEnvelope());
//                }
//                if (Objects.equals(contentType, JSON_TYPE)) {
//                    Object evaluatedResult = aggregationExpression.objectValueOf(synCtx);
//                    if (evaluatedResult instanceof JsonElement) {
//                        jsonArray.add((JsonElement) evaluatedResult);
//                    } else {
//                        handleException(aggregate, "Error merging aggregation results as expression : " +
//                                aggregationExpression.toString() + " did not resolve to a JSON value", null, synCtx);
//                    }
//                } else {
//                    enrichEnvelope(synCtx, aggregationExpression);
//                }
//            } else {
//                try {
//                    if (log.isDebugEnabled()) {
//                        log.debug("Merging message : " + synCtx.getEnvelope() + " using expression : " +
//                                aggregationExpression);
//                    }
//                    if (Objects.equals(contentType, JSON_TYPE)) {
//                        Object evaluatedResult = aggregationExpression.objectValueOf(synCtx);
//                        if (evaluatedResult instanceof JsonElement) {
//                            jsonArray.add((JsonElement) evaluatedResult);
//                        } else {
//                            jsonArray.add(evaluatedResult.toString());
//                        }
//                    } else {
//                        enrichEnvelope(synCtx, aggregationExpression);
//                    }
//
//                    if (log.isDebugEnabled()) {
//                        log.debug("Merged result : " + newCtx.getEnvelope());
//                    }
//                } catch (SynapseException e) {
//                    handleException(aggregate, "Error evaluating expression: " + aggregationExpression.toString(), e, synCtx);
//                } catch (JsonSyntaxException e) {
//                    handleException(aggregate, "Error reading JSON element: " + aggregationExpression.toString(), e, synCtx);
//                }
//            }
//        }
//
//        StatisticDataCollectionHelper.collectAggregatedParents(aggregate.getMessages(), newCtx);
//        if (Objects.equals(contentType, JSON_TYPE)) {
//            // setting the new JSON payload to the messageContext
//            try {
//                JsonUtil.getNewJsonPayload(((Axis2MessageContext) newCtx).getAxis2MessageContext(), new
//                        ByteArrayInputStream(jsonArray.toString().getBytes()), true, true);
//            } catch (AxisFault axisFault) {
//                log.error("Error occurred while setting the new JSON payload to the msg context", axisFault);
//            }
//        } else {
//            // Removing the JSON stream after aggregated using XML path.
//            // This will fix inconsistent behaviour in logging the payload.
//            ((Axis2MessageContext) newCtx).getAxis2MessageContext()
//                    .removeProperty(org.apache.synapse.commons.json.Constants.ORG_APACHE_SYNAPSE_COMMONS_JSON_JSON_INPUT_STREAM);
//        }
//        return newCtx;
//    }

    public long getCompletionTimeoutMillis() {

        return completionTimeoutMillis;
    }

    public void setCompletionTimeoutMillis(long completionTimeoutMillis) {

        this.completionTimeoutMillis = completionTimeoutMillis;
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
//        for (Target target : targets) {
        target.setStatisticIdForMediators(holder);
//        }

        StatisticIdentityGenerator.reportingFlowContinuableEndEvent(sequenceId, ComponentType.MEDIATOR, holder);
    }

    @Override
    public boolean isContentAltering() {

        return true;
    }

    private void handleException(Aggregate aggregate, String msg, Exception exception, MessageContext msgContext) {

        aggregate.clear();
        activeAggregates.clear();
        if (exception != null) {
            super.handleException(msg, exception, msgContext);
        } else {
            super.handleException(msg, msgContext);
        }
    }

    public String getContentType() {

        return contentType;
    }

    private void setContentType(MessageContext synCtx) {

        org.apache.axis2.context.MessageContext a2mc = ((Axis2MessageContext) synCtx).getAxis2MessageContext();
        if (Objects.equals(contentType, JSON_TYPE)) {
            a2mc.setProperty(Constants.Configuration.MESSAGE_TYPE, JSON_CONTENT_TYPE);
            a2mc.setProperty(Constants.Configuration.CONTENT_TYPE, JSON_CONTENT_TYPE);
            setContentTypeHeader(JSON_CONTENT_TYPE, a2mc);
        } else {
            a2mc.setProperty(Constants.Configuration.MESSAGE_TYPE, XML_CONTENT_TYPE);
            a2mc.setProperty(Constants.Configuration.CONTENT_TYPE, XML_CONTENT_TYPE);
            setContentTypeHeader(XML_CONTENT_TYPE, a2mc);
        }
        a2mc.removeProperty("NO_ENTITY_BODY");
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

    private boolean isTargetBody() {

        return "body".equalsIgnoreCase(resultTarget);
    }
}
