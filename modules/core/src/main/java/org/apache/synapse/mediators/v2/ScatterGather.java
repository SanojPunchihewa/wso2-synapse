package org.apache.synapse.mediators.v2;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.OperationContext;
import org.apache.synapse.ContinuationState;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.aspects.flow.statistics.collectors.RuntimeStatisticCollector;
import org.apache.synapse.aspects.flow.statistics.util.StatisticDataCollectionHelper;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.config.xml.SynapsePath;
import org.apache.synapse.continuation.ContinuationStackManager;
import org.apache.synapse.continuation.ReliantContinuationState;
import org.apache.synapse.continuation.SeqContinuationState;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.FlowContinuableMediator;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.eip.EIPConstants;
import org.apache.synapse.mediators.eip.EIPUtils;
import org.apache.synapse.mediators.eip.SharedDataHolder;
import org.apache.synapse.mediators.eip.Target;
import org.apache.synapse.mediators.eip.aggregator.Aggregate;
import org.apache.synapse.util.MessageHelper;
import org.apache.synapse.util.xpath.SynapseJsonPath;
import org.apache.synapse.util.xpath.SynapseXPath;
import org.jaxen.JaxenException;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;

public class ScatterGather extends AbstractMediator implements ManagedLifecycle, FlowContinuableMediator {

    private final String id;
    private final Object lock = new Object();
    /**
     * Reference to the synapse environment
     */
    private SynapseEnvironment synapseEnv;
    /**
     * the list of targets to which cloned copies of the message will be given for mediation
     */
    private List<Target> targets = new ArrayList<Target>();
    private Map<String, Aggregate> activeAggregates = Collections.synchronizedMap(new HashMap<>());
    private long completionTimeoutMillis = 0;
    /**
     * The maximum number of messages required to complete aggregation
     */
    private Value minMessagesToComplete;
    /**
     * The minimum number of messages required to complete aggregation
     */
    private Value maxMessagesToComplete;
    private boolean isAggregateComplete = false;

    private SynapsePath correlateExpression = null;
    private boolean sequential = true;
    /**
     * An XPath expression that may specify a selected element to be aggregated from a group of
     * messages to create the aggregated message
     * e.g. //getQuote/return would pick up and aggregate the //getQuote/return elements from a
     * bunch of matching messages into one aggregated message
     */
    private SynapsePath aggregationExpression = null;

    public ScatterGather() {
        // set id to a random UUID to be used for correlation
        id = String.valueOf(new Random().nextLong());
//        aggregateMediator.setId(id);
    }

    public void setSequential(boolean isSequential) {

        this.sequential = isSequential;
    }

    public String getId() {

        return id;
    }

    @Override
    public boolean mediate(MessageContext synCtx) {

        boolean aggregationResult = false;

        SynapseLog synLog = getLog(synCtx);

        if (synLog.isTraceOrDebugEnabled()) {
            synLog.traceOrDebug("Start : Clone mediator");

            if (synLog.isTraceTraceEnabled()) {
                synLog.traceTrace("Message : " + synCtx.getEnvelope());
            }
        }

        // get the targets list, clone the message for the number of targets and then
        // mediate the cloned messages using the targets
        Iterator<Target> iter = targets.iterator();
        int i = 0;
//        boolean isStopFlowOnFailure = "true".equalsIgnoreCase((String)
//                synCtx.getProperty(STOP_FLOW_ON_FAILURE_PROPERTY_NAME));
        while (iter.hasNext()) {
//            if (synLog.isTraceOrDebugEnabled()) {
//                synLog.traceOrDebug("Submitting " + (i + 1) + " of " + targets.size() +
//                        " messages for " + (isSequential() ? "sequential processing" : "parallel processing"));
//            }

            MessageContext clonedMsgCtx = getClonedMessageContext(synCtx, i++, targets.size());
            ContinuationStackManager.addReliantContinuationState(clonedMsgCtx, i - 1,
                    getMediatorPosition());
            boolean result = iter.next().mediate(clonedMsgCtx);
            if (sequential && result) {
                aggregationResult = aggregateMessages(clonedMsgCtx, synLog);
            }
//            aggregateMediator.mediate(clonedMsgCtx);
//            boolean isFailure = "true".equalsIgnoreCase((String)clonedMsgCtx.
//                    getProperty(EIPConstants.ERROR_ON_TARGET_EXECUTION));
//            if (isFailure && sequential && isStopFlowOnFailure) {
//                break;
//            }
        }
        // if the continuation of the parent message is stopped from here set the RESPONSE_WRITTEN
        // property to SKIP to skip the blank http response
        OperationContext opCtx
                = ((Axis2MessageContext) synCtx).getAxis2MessageContext().getOperationContext();
        if (opCtx != null) {
            opCtx.setProperty(Constants.RESPONSE_WRITTEN, "SKIP");
        }
        return aggregationResult;
    }

    public void init(SynapseEnvironment se) {

        synapseEnv = se;
        for (Target target : targets) {
            ManagedLifecycle seq = target.getSequence();
            if (seq != null) {
                seq.init(se);
            }
        }
    }

    public void destroy() {

        for (Target target : targets) {
            ManagedLifecycle seq = target.getSequence();
            if (seq != null) {
                seq.destroy();
            }
        }
    }

    /**
     * clone the provided message context as a new message, and mark as the messageSequence'th
     * message context of a total of messageCount messages
     *
     * @param synCtx          - MessageContext which is subjected to the cloning
     * @param messageSequence - the position of this message of the cloned set
     * @param messageCount    - total of cloned copies
     * @return MessageContext the cloned message context
     */
    private MessageContext getClonedMessageContext(MessageContext synCtx, int messageSequence,
                                                   int messageCount) {

        MessageContext newCtx = null;
        try {

            newCtx = MessageHelper.cloneMessageContext(synCtx);

            // Set isServerSide property in the cloned message context
            ((Axis2MessageContext) newCtx).getAxis2MessageContext().setServerSide(
                    ((Axis2MessageContext) synCtx).getAxis2MessageContext().isServerSide());

            if (id != null) {
                // set the parent correlation details to the cloned MC -
                //                              for the use of aggregation like tasks
                newCtx.setProperty(EIPConstants.AGGREGATE_CORRELATION + "." + id,
                        synCtx.getMessageID());
                // set the property MESSAGE_SEQUENCE to the MC for aggregation purposes
                newCtx.setProperty(EIPConstants.MESSAGE_SEQUENCE + "." + id,
                        String.valueOf(messageSequence) + EIPConstants.MESSAGE_SEQUENCE_DELEMITER +
                                messageCount);
            } else {
                newCtx.setProperty(EIPConstants.MESSAGE_SEQUENCE,
                        String.valueOf(messageSequence) + EIPConstants.MESSAGE_SEQUENCE_DELEMITER +
                                messageCount);
            }
        } catch (AxisFault axisFault) {
            handleException("Error cloning the message context", axisFault, synCtx);
        }

        return newCtx;
    }

    public List<Target> getTargets() {

        return targets;
    }

    public void setTargets(List<Target> targets) {

        this.targets = targets;
    }

    public void addTarget(Target target) {

        this.targets.add(target);
    }

    public SynapsePath getAggregationExpression() {
//        return aggregateMediator.getAggregationExpression();
        return aggregationExpression;
    }

    public void setAggregationExpression(SynapsePath aggregationExpression) {

        this.aggregationExpression = aggregationExpression;
//        this.aggregateMediator.setAggregationExpression(aggregationExpression);
    }

    @Override
    public boolean mediate(MessageContext synCtx, ContinuationState continuationState) {

        SynapseLog synLog = getLog(synCtx);

        if (synLog.isTraceOrDebugEnabled()) {
            synLog.traceOrDebug("Clone mediator : Mediating from ContinuationState");
        }

        boolean result;
        int subBranch = ((ReliantContinuationState) continuationState).getSubBranch();

        SequenceMediator branchSequence = targets.get(subBranch).getSequence();
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
        if (isStatisticsEnabled) {
            branchSequence.reportCloseStatistics(synCtx, null);
        }
//        return aggregateMediator.mediate(synCtx);
        return aggregateMessages(synCtx, synLog);
    }

    private boolean aggregateMessages(MessageContext synCtx, SynapseLog synLog) {

        Aggregate aggregate = null;
        String correlationIdName = (id != null ? EIPConstants.AGGREGATE_CORRELATION + "." + id :
                EIPConstants.AGGREGATE_CORRELATION);

        Object o = synCtx.getProperty(correlationIdName);
        String correlation;

        if (o != null && o instanceof String) {
            correlation = (String) o;
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

                        Double minMsg = -1.0;
                        if (minMessagesToComplete != null) {
                            minMsg = Double.parseDouble(minMessagesToComplete.evaluateValue(synCtx));
                        }
                        Double maxMsg = -1.0;
                        if (maxMessagesToComplete != null) {
                            maxMsg = Double.parseDouble(maxMessagesToComplete.evaluateValue(synCtx));
                        }

                        aggregate = new Aggregate(
                                synCtx.getEnvironment(),
                                correlation,
                                completionTimeoutMillis,
                                minMsg.intValue(),
                                maxMsg.intValue(), this, synCtx.getFaultStack().peek());

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
            synLog.traceOrDebug("Unable to find aggrgation correlation property");
            return true;
        }
        // if there is an aggregate continue on aggregation
        if (aggregate != null) {
            //this is a temporary fix
            synCtx.getEnvelope().build();
            boolean collected = aggregate.addMessage(synCtx);
            if (synLog.isTraceOrDebugEnabled()) {
                if (collected) {
                    synLog.traceOrDebug("Collected a message during aggregation");
                    if (synLog.isTraceTraceEnabled()) {
                        synLog.traceTrace("Collected message : " + synCtx);
                    }
                }
            }

            // check the completeness of the aggregate and if completed aggregate the messages
            // if not completed return false and block the message sequence till it completes

            if (aggregate.isComplete(synLog)) {
                synLog.traceOrDebug("Aggregation completed - invoking onComplete");
                boolean onCompleteSeqResult = completeAggregate(aggregate);
                synLog.traceOrDebug("End : Aggregate mediator");
                isAggregateComplete = onCompleteSeqResult;
                return onCompleteSeqResult;
            } else {
                aggregate.releaseLock();
            }

        } else {
            // if the aggregation correlation cannot be found then continue the message on the
            // normal path by returning true

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

        MessageContext newSynCtx = getAggregatedMessage(aggregate);

        if (newSynCtx == null) {
            log.warn("An aggregation of messages timed out with no aggregated messages", null);
            return false;
        } else {
//            isAggregationMessageCollected = true;
            // Get the aggregated message to the next mediator placed after the aggregate mediator
            // in the sequence
            if (newSynCtx.isContinuationEnabled()) {
                try {
                    aggregate.getLastMessage().setEnvelope(
                            MessageHelper.cloneSOAPEnvelope(newSynCtx.getEnvelope()));
                    // Setting the new JSON stream to the next mediator
                    if (aggregationExpression instanceof SynapseJsonPath) {
                        org.apache.axis2.context.MessageContext msgCtx = ((Axis2MessageContext) aggregate.getLastMessage())
                                .getAxis2MessageContext();
                        org.apache.axis2.context.MessageContext newAxisMsgCtx = ((Axis2MessageContext) newSynCtx)
                                .getAxis2MessageContext();
                        msgCtx.setProperty(org.apache.synapse.commons.json.Constants.ORG_APACHE_SYNAPSE_COMMONS_JSON_JSON_INPUT_STREAM,
                                newAxisMsgCtx.getProperty(org.apache.synapse.commons.json.Constants.ORG_APACHE_SYNAPSE_COMMONS_JSON_JSON_INPUT_STREAM));
                    }
                } catch (AxisFault axisFault) {
                    log.warn("Error occurred while assigning aggregated message" +
                            " back to the last received message context");
                }
            }
        }

        aggregate.clear();
        activeAggregates.remove(aggregate.getCorrelation());

//        if ((correlateExpression != null &&
//                correlateExpression.toString().equals(aggregate.getCorrelation())) ||
//                correlateExpression == null) {
//            return true;
//        }
//        return false;
        SeqContinuationState seqContinuationState = (SeqContinuationState) ContinuationStackManager.peakContinuationStateStack(newSynCtx);
        if (seqContinuationState == null) {
            return false;
        }
        boolean result = false;
        do {
            seqContinuationState = (SeqContinuationState) ContinuationStackManager.peakContinuationStateStack(newSynCtx);
            if (seqContinuationState != null) {
                SequenceMediator sequenceMediator = ContinuationStackManager.retrieveSequence(newSynCtx, seqContinuationState);
                //Report Statistics for this continuation call
                result = sequenceMediator.mediate(newSynCtx, seqContinuationState);
                if (RuntimeStatisticCollector.isStatisticsEnabled()) {
                    sequenceMediator.reportCloseStatistics(newSynCtx, null);
                }
            } else {
                break;
            }
            //for any result close the sequence as it will be handled by the callback method in statistics
        } while (result && !newSynCtx.getContinuationStateStack().isEmpty());
        return result;
    }

    private MessageContext getAggregatedMessage(Aggregate aggregate) {

        MessageContext newCtx = null;
        JsonArray jsonArray = new JsonArray();
        JsonElement result;
        boolean isJSONAggregation = aggregationExpression instanceof SynapseJsonPath;

        JsonObject resultJSONObject = new JsonObject();

        for (MessageContext synCtx : aggregate.getMessages()) {

            if (newCtx == null) {
                try {
                    newCtx = MessageHelper.cloneMessageContext(synCtx, true, false, true);
                } catch (AxisFault axisFault) {
                    handleException(aggregate, "Error creating a copy of the message", axisFault, synCtx);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Generating Aggregated message from : " + newCtx.getEnvelope());
                }
                if (isJSONAggregation) {
                    jsonArray.add(EIPUtils.getJSONElement(synCtx, (SynapseJsonPath) aggregationExpression));
                } else {
                    try {
                        EIPUtils.enrichEnvelope(newCtx.getEnvelope(), synCtx, (SynapseXPath) aggregationExpression);
                    } catch (JaxenException e) {
                        handleException(aggregate, "Error merging aggregation results using XPath : " +
                                aggregationExpression.toString(), e, synCtx);
                    }
                }
            } else {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Merging message : " + synCtx.getEnvelope() + " using XPath : " +
                                aggregationExpression);
                    }
                    if (isJSONAggregation) {
                        jsonArray.add(EIPUtils.getJSONElement(synCtx, (SynapseJsonPath) aggregationExpression));
                    } else {
                        EIPUtils.enrichEnvelope(newCtx.getEnvelope(), synCtx.getEnvelope(), synCtx, (SynapseXPath)
                                aggregationExpression);
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Merged result : " + newCtx.getEnvelope());
                    }

                } catch (JaxenException e) {
                    handleException(aggregate, "Error merging aggregation results using XPath : " +
                            aggregationExpression.toString(), e, synCtx);
                } catch (SynapseException e) {
                    handleException(aggregate, "Error evaluating expression: " + aggregationExpression.toString(), e, synCtx);
                } catch (JsonSyntaxException e) {
                    handleException(aggregate, "Error reading JSON element: " + aggregationExpression.toString(), e, synCtx);
                }
            }
        }

        // setting json array as the result
        result = jsonArray;

        StatisticDataCollectionHelper.collectAggregatedParents(aggregate.getMessages(), newCtx);
        if (isJSONAggregation) {
            // setting the new JSON payload to the messageContext
            try {
                JsonUtil.getNewJsonPayload(((Axis2MessageContext) newCtx).getAxis2MessageContext(), new
                        ByteArrayInputStream(result.toString().getBytes()), true, true);
            } catch (AxisFault axisFault) {
                log.error("Error occurred while setting the new JSON payload to the msg context", axisFault);
            }
        } else {
            // Removing the JSON stream after aggregated using XML path.
            // This will fix inconsistent behaviour in logging the payload.
            ((Axis2MessageContext) newCtx).getAxis2MessageContext()
                    .removeProperty(org.apache.synapse.commons.json.Constants.ORG_APACHE_SYNAPSE_COMMONS_JSON_JSON_INPUT_STREAM);
        }
        return newCtx;
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
}
