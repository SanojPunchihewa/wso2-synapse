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

import org.apache.synapse.ContinuationState;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.aspects.AspectConfiguration;
import org.apache.synapse.aspects.ComponentType;
import org.apache.synapse.aspects.flow.statistics.StatisticIdentityGenerator;
import org.apache.synapse.aspects.flow.statistics.collectors.RuntimeStatisticCollector;
import org.apache.synapse.aspects.flow.statistics.data.artifact.ArtifactHolder;
import org.apache.synapse.config.xml.AnonymousListMediator;
import org.apache.synapse.config.xml.SynapsePath;
import org.apache.synapse.continuation.ContinuationStackManager;
import org.apache.synapse.continuation.ReliantContinuationState;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.AbstractListMediator;
import org.apache.synapse.mediators.FlowContinuableMediator;
import org.apache.synapse.mediators.ListMediator;
import org.apache.synapse.util.xpath.SynapseJsonPath;
import org.apache.synapse.util.xpath.SynapseXPath;
import org.jaxen.JaxenException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IfElseMediator extends AbstractListMediator implements FlowContinuableMediator {

    private SynapsePath valueToCompare = null;
    private Pattern regex = null;
    private SynapsePath condition = null;
    private AnonymousListMediator elseMediator = null;
    private boolean thenElementPresent = false;

    @Override
    public void init(SynapseEnvironment se) {

        super.init(se);
        if (elseMediator != null) {
            elseMediator.init(se);
        }
    }

    @Override
    public void destroy() {

        super.destroy();
        if (elseMediator != null) {
            elseMediator.destroy();
        }
    }

    /**
     * Executes the list of sub/child mediators, if the condition is satisfied
     *
     * @param synCtx the current message
     * @return true if condition fails. else returns as per List mediator semantics
     */
    public boolean mediate(MessageContext synCtx) {

        if (synCtx.getEnvironment().isDebuggerEnabled()) {
            if (super.divertMediationRoute(synCtx)) {
                return true;
            }
        }

        SynapseLog synLog = getLog(synCtx);

        if (synLog.isTraceOrDebugEnabled()) {
            synLog.traceOrDebug("Start : If Else mediator");

            if (synLog.isTraceTraceEnabled()) {
                synLog.traceTrace("Message : " + synCtx.getEnvelope());
            }
        }

        boolean result;
        if (checkCondition(synCtx)) {
            if (synLog.isTraceOrDebugEnabled()) {
                synLog.traceOrDebug((condition == null ?
                        "Value to compare : " + valueToCompare + " against : " + regex.pattern() + " matches" :
                        "Expression : " + condition + " evaluates to true") +
                        " - executing child mediators");
            }
            ContinuationStackManager.
                    addReliantContinuationState(synCtx, 0, getMediatorPosition());
            result = super.mediate(synCtx);
            if (result) {
                ContinuationStackManager.removeReliantContinuationState(synCtx);
            }
        } else {
            if (elseMediator != null) {
                if (synLog.isTraceOrDebugEnabled()) {
                    synLog.traceOrDebug((condition == null ?
                            "Value to compare : " + valueToCompare + " against : " + regex.pattern() + " does not match" :
                            "Expression : " + condition + " evaluates to false") +
                            " - executing the else path child mediators");
                }
                ContinuationStackManager.addReliantContinuationState(synCtx, 1, getMediatorPosition());
                result = elseMediator.mediate(synCtx);
                if (result) {
                    ContinuationStackManager.removeReliantContinuationState(synCtx);
                }
            } else {
                result = true;
            }
        }
        synLog.traceOrDebug("End : If Else mediator ");
        return result;
    }

    public boolean mediate(MessageContext synCtx,
                           ContinuationState continuationState) {

        SynapseLog synLog = getLog(synCtx);

        if (synLog.isTraceOrDebugEnabled()) {
            synLog.traceOrDebug("If Else mediator : Mediating from ContinuationState");
        }

        boolean result;
        int subBranch = ((ReliantContinuationState) continuationState).getSubBranch();
        boolean isStatisticsEnabled = RuntimeStatisticCollector.isStatisticsEnabled();
        if (subBranch == 0) {
            if (!continuationState.hasChild()) {
                result = super.mediate(synCtx, continuationState.getPosition() + 1);
            } else {
                FlowContinuableMediator mediator =
                        (FlowContinuableMediator) getChild(continuationState.getPosition());

                result = mediator.mediate(synCtx, continuationState.getChildContState());

                if (isStatisticsEnabled) {
                    ((Mediator) mediator).reportCloseStatistics(synCtx, null);
                }
            }
        } else {
            if (!continuationState.hasChild()) {
                result = elseMediator.mediate(synCtx, continuationState.getPosition() + 1);
            } else {
                FlowContinuableMediator mediator =
                        (FlowContinuableMediator) elseMediator.getChild(
                                continuationState.getPosition());

                result = mediator.mediate(synCtx, continuationState.getChildContState());

                if (isStatisticsEnabled) {
                    ((Mediator) mediator).reportCloseStatistics(synCtx, null);
                }
            }
            if (isStatisticsEnabled) {
                elseMediator.reportCloseStatistics(synCtx, null);
            }
        }
        return result;
    }

    /**
     * Tests the supplied condition after evaluation against the given Expression
     * or Regex (against a source expression). When a regular expression is supplied
     * the source expression is evaluated into a String value, and matched against
     * the given regex
     *
     * @param synCtx the current message for evaluation of the test condition
     * @return true if evaluation of the Expression/Regex results in true
     */
    private boolean checkCondition(MessageContext synCtx) {

        SynapseLog synLog = getLog(synCtx);

        if (condition != null) {
            try {
                if (condition instanceof SynapseXPath) {
                    return condition.booleanValueOf(synCtx);
                } else if (condition instanceof SynapseJsonPath) {
                    return ((SynapseJsonPath) condition).booleanValueOf(synCtx);
                }
            } catch (JaxenException e) {
                handleException("Error evaluating condition expression : " + condition, e, synCtx);
            }

        } else if (valueToCompare != null && regex != null) {
            String sourceString = valueToCompare.stringValueOf(synCtx);
            if (sourceString == null) {
                if (synLog.isTraceOrDebugEnabled()) {
                    synLog.traceOrDebug("Value to compare String : " + valueToCompare + " evaluates to null");
                }
                return false;
            }
            Matcher matcher = regex.matcher(sourceString);
            if (matcher == null) {
                if (synLog.isTraceOrDebugEnabled()) {
                    synLog.traceOrDebug("Regex pattern matcher for : " + regex.pattern() +
                            "against Value to compare : " + sourceString + " is null");
                }
                return false;
            }
            return matcher.matches();
        }

        return false; // never executes
    }

    public Pattern getRegex() {

        return regex;
    }

    public void setRegex(Pattern regex) {

        this.regex = regex;
    }

    public SynapsePath getValueToCompare() {

        return valueToCompare;
    }

    public void setValueToCompare(SynapsePath valueToCompare) {

        this.valueToCompare = valueToCompare;
    }

    public SynapsePath getCondition() {

        return condition;
    }

    public void setCondition(SynapsePath condition) {

        this.condition = condition;
    }

    public ListMediator getElseMediator() {

        return elseMediator;
    }

    public void setElseMediator(AnonymousListMediator elseMediator) {

        this.elseMediator = elseMediator;
    }

    public boolean isThenElementPresent() {

        return thenElementPresent;
    }

    public void setThenElementPresent(boolean thenElementPresent) {

        this.thenElementPresent = thenElementPresent;
    }

    @Override
    public boolean isContentAware() {

        if (condition != null) {
            return condition.isContentAware();
        } else if (valueToCompare != null) {
            return valueToCompare.isContentAware();
        }
        return false;
    }

    @Override
    public void setComponentStatisticsId(ArtifactHolder holder) {

        if (getAspectConfiguration() == null) {
            configure(new AspectConfiguration(getMediatorName()));
        }
        String mediatorId =
                StatisticIdentityGenerator.getIdForFlowContinuableMediator(getMediatorName(), ComponentType.MEDIATOR, holder);
        getAspectConfiguration().setUniqueId(mediatorId);

        StatisticIdentityGenerator.reportingBranchingEvents(holder);
        setStatisticIdForMediators(holder);
        StatisticIdentityGenerator.reportingEndBranchingEvent(holder);

        StatisticIdentityGenerator.reportingBranchingEvents(holder);
        if (elseMediator != null) {
            elseMediator.setStatisticIdForMediators(holder);
        }
        StatisticIdentityGenerator.reportingFlowContinuableEndEvent(mediatorId, ComponentType.MEDIATOR, holder);
        StatisticIdentityGenerator.reportingEndBranchingEvent(holder);
    }
}
