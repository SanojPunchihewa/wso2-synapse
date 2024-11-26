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

package org.apache.synapse.mediators.eip.aggregator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.FaultHandler;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.eip.EIPConstants;
import org.apache.synapse.mediators.v2.ForEachMediator;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

/**
 * An instance of this class is created to manage each aggregation group, and it holds
 * the aggregation properties and the messages collected during aggregation. This class also
 * times out itself after the timeout expires it
 */
public class ForLoopAggregate extends TimerTask {

    private static final Log log = LogFactory.getLog(ForLoopAggregate.class);

    private long timeoutMillis = 0;
    /**
     * The time in millis at which this aggregation should be considered as expired
     */
    private long expiryTimeMillis = 0;
    /**
     * The minimum number of messages to be collected to consider this aggregation as complete
     */
    private ForEachMediator forLoopMediator = null;
    private List<MessageContext> messages = new ArrayList<MessageContext>();
    private boolean locked = false;
    private boolean completed = false;
    private SynapseEnvironment synEnv = null;
    private String correlation = null;

    /**
     * Fault handler for the aggregate mediator
     */
    private FaultHandler faultHandler;

    public ForLoopAggregate(SynapseEnvironment synEnv, String correlation, long timeoutMillis, ForEachMediator forLoopMediator,
                            FaultHandler faultHandler) {

        this.synEnv = synEnv;
        this.correlation = correlation;
        if (timeoutMillis > 0) {
            expiryTimeMillis = System.currentTimeMillis() + timeoutMillis;
        }
        this.faultHandler = faultHandler;
        this.forLoopMediator = forLoopMediator;
    }

    /**
     * Add a message to the interlan message list
     *
     * @param synCtx message to be added into this aggregation group
     * @return true if the message was added or false if not
     */
    public synchronized boolean addMessage(MessageContext synCtx) {

            if (messages == null) {
                return false;
            }
            messages.add(synCtx);
            return true;
    }

    /**
     * Has this aggregation group completed?
     *
     * @param synLog the Synapse log to use
     * @return boolean true if aggregation is complete
     */
    public synchronized boolean isComplete(SynapseLog synLog) {

        if (!completed) {

            // if any messages have been collected, check if the completion criteria is met
            if (!messages.isEmpty()) {

                // get total messages for this group, from the first message we have collected
                MessageContext mc = messages.get(0);
                Object prop = mc.getProperty(EIPConstants.MESSAGE_SEQUENCE + "." + forLoopMediator.getId());

                if (prop != null && prop instanceof String) {
                    String[] msgSequence = prop.toString().split(
                            EIPConstants.MESSAGE_SEQUENCE_DELEMITER);
                    int total = Integer.parseInt(msgSequence[1]);

                    if (synLog.isTraceOrDebugEnabled()) {
                        synLog.traceOrDebug(messages.size() +
                                " messages of " + total + " collected in current aggregation");
                    }

                    if (messages.size() >= total) {
                        synLog.traceOrDebug("Aggregation complete");
                        return true;
                    }
                }
            } else {
                synLog.traceOrDebug("No messages collected in current aggregation");
            }
            // else, has this aggregation reached its timeout?
            if (expiryTimeMillis > 0 && System.currentTimeMillis() >= expiryTimeMillis) {
                synLog.traceOrDebug("Aggregation complete - the aggregation has timed out");
                return true;
            }
        } else {
            synLog.traceOrDebug(
                    "Aggregation already completed - this message will not be processed in aggregation");
        }

        return false;
    }

    public MessageContext getLastMessage() {

        return messages.get(messages.size() - 1);
    }

    public long getTimeoutMillis() {

        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {

        this.timeoutMillis = timeoutMillis;
    }

    public synchronized List<MessageContext> getMessages() {

        return new ArrayList<MessageContext>(messages);
    }

    public void setMessages(List<MessageContext> messages) {

        this.messages = messages;
    }

    public long getExpiryTimeMillis() {

        return expiryTimeMillis;
    }

    public void setExpiryTimeMillis(long expiryTimeMillis) {

        this.expiryTimeMillis = expiryTimeMillis;
    }

    public String getCorrelation() {
        return correlation;
    }

    public void run() {

        while (true) {
            if (completed) {
                break;
            }
            if (getLock()) {
                if (log.isDebugEnabled()) {
                    log.debug("Time : " + System.currentTimeMillis() + " and this aggregator " +
                            "expired at : " + expiryTimeMillis);
                }
                synEnv.getExecutorService().execute(new AggregateTimeout(this));
                break;
            }
        }
    }

    /**
     * Clear references in Aggregate Timer Task
     * <p>
     * This need to be called when aggregation is completed.
     * Task is not eligible for gc until it reach the execution time,
     * even though it is cancelled. So we need to remove references from task to other objects to
     * allow them to be garbage collected
     */
    public void clear() {

        messages = null;
    }

    public synchronized boolean getLock() {

        return !locked;
    }

    public void releaseLock() {

        locked = false;
    }

    public boolean isCompleted() {

        return completed;
    }

    public void setCompleted(boolean completed) {

        this.completed = completed;
    }

    private class AggregateTimeout implements Runnable {

        private ForLoopAggregate aggregate = null;

        AggregateTimeout(ForLoopAggregate aggregate) {

            this.aggregate = aggregate;
        }

        public void run() {

            MessageContext messageContext = aggregate.getLastMessage();
            try {
                    log.warn("Aggregate mediator timeout occurred.");
                    forLoopMediator.completeAggregate(aggregate);
            } catch (Exception ex) {
                if (faultHandler != null && messageContext != null) {
                    faultHandler.handleFault(messageContext, ex);
                } else {
                    log.error("Synapse encountered an exception, No error handlers found or no messages were " +
                            "aggregated - [Message Dropped]\n" + ex.getMessage());
                }
            }
        }
    }

}
