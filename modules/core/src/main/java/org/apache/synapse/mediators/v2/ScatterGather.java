package org.apache.synapse.mediators.v2;

import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.OperationContext;
import org.apache.synapse.ContinuationState;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.aspects.flow.statistics.collectors.RuntimeStatisticCollector;
import org.apache.synapse.config.xml.SynapsePath;
import org.apache.synapse.continuation.ContinuationStackManager;
import org.apache.synapse.continuation.ReliantContinuationState;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.endpoints.Endpoint;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.FlowContinuableMediator;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.eip.EIPConstants;
import org.apache.synapse.mediators.eip.Target;
import org.apache.synapse.mediators.eip.aggregator.AggregateMediator;
import org.apache.synapse.util.MessageHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class ScatterGather extends AbstractMediator implements ManagedLifecycle, FlowContinuableMediator {

    private final String id;
    /** Reference to the synapse environment */
    private SynapseEnvironment synapseEnv;

    /** the list of targets to which cloned copies of the message will be given for mediation */
    private List<Target> targets = new ArrayList<Target>();
    private final AggregateMediator aggregateMediator = new AggregateMediator();

    public ScatterGather() {
        // set id to a random UUID to be used for correlation
        id = String.valueOf(new Random().nextLong());
        aggregateMediator.setId(id);
    }

    @Override
    public boolean mediate(MessageContext synCtx) {

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
            iter.next().mediate(clonedMsgCtx);
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
        return false;
    }

    public void init(SynapseEnvironment se) {

        synapseEnv = se;
        for (Target target : targets) {
            ManagedLifecycle seq = target.getSequence();
            if (seq != null) {
                seq.init(se);
            } else if (target.getSequenceRef() != null) {
                SequenceMediator targetSequence =
                        (SequenceMediator) se.getSynapseConfiguration().
                                getSequence(target.getSequenceRef());

                if (targetSequence == null || targetSequence.isDynamic()) {
                    se.addUnavailableArtifactRef(target.getSequenceRef());
                }
            }
            Endpoint endpoint = target.getEndpoint();
            if (endpoint != null) {
                endpoint.init(se);
            }
        }
    }

    public void destroy() {

        for (Target target : targets) {
            ManagedLifecycle seq = target.getSequence();
            if (seq != null) {
                seq.destroy();
            } else if (target.getSequenceRef() != null) {
                SequenceMediator targetSequence =
                        (SequenceMediator) synapseEnv.getSynapseConfiguration().
                                getSequence(target.getSequenceRef());

                if (targetSequence == null || targetSequence.isDynamic()) {
                    synapseEnv.removeUnavailableArtifactRef(target.getSequenceRef());
                }
            }
            Endpoint endpoint = target.getEndpoint();
            if (endpoint != null) {
                endpoint.destroy();
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
     *
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
//        target.getSequence().addChild(aggregateMediator);
        this.targets.add(target);
    }

//    public String getId() {
//        return id;
//    }
//
//    public void setId(String id) {
//        this.id = id;
//        this.aggregateMediator.setId(id);
//    }

    public SynapsePath getAggregationExpression() {
        return aggregateMediator.getAggregationExpression();
    }

    public void setAggregationExpression(SynapsePath aggregationExpression) {
        this.aggregateMediator.setAggregationExpression(aggregationExpression);
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
        return aggregateMediator.mediate(synCtx);
    }
}
