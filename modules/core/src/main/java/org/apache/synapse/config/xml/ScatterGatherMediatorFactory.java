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
import org.apache.axis2.util.JavaUtils;
import org.apache.synapse.Mediator;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.eip.Target;
import org.apache.synapse.mediators.eip.splitter.CloneMediator;
import org.apache.synapse.mediators.v2.ScatterGather;
import org.apache.synapse.util.xpath.SynapseJsonPath;
import org.apache.synapse.util.xpath.SynapseXPath;
import org.jaxen.JaxenException;

import java.util.Iterator;
import java.util.Properties;
import javax.xml.namespace.QName;

/**
 * The &lt;clone&gt; element is used to copy messages in Synapse to similar messages but with
 * different message contexts and mediated using the specified targets
 *
 * <pre>
 * &lt;clone [continueParent=(true | false)] [iterations="number"]&gt;
 *   &lt;target [to="uri"] [soapAction="qname"] [sequence="sequence_ref"]
 *          [endpoint="endpoint_ref"]&gt;
 *     &lt;sequence&gt;
 *       (mediator)+
 *     &lt;/sequence&gt;?
 *     &lt;endpoint&gt;
 *       endpoint
 *     &lt;/endpoint&gt;?
 *   &lt;/target&gt;+
 * &lt;/clone&gt;
 * </pre>
 */
public class ScatterGatherMediatorFactory extends AbstractMediatorFactory {

    /**
     * This will hold the QName of the clone mediator element in the xml configuration
     */
    private static final QName SCATTER_GATHER_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "scatter-gather");
    private static final QName ATT_VALUE_TO_AGGREGATE = new QName("value-to-aggregate");
    private static final QName ATT_CONDITION = new QName("condition");
    private static final QName ATT_TIMEOUT = new QName("timeout");
    private static final QName ATT_MIN_MESSAGES = new QName("min-messages");
    private static final QName ATT_MAX_MESSAGES = new QName("max-messages");
    private static final QName ATT_TARGET_VARIABLE = new QName("target-variable");

    private static final QName TARGET_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "target");
    private static final QName SEQUENTIAL_Q = new QName("sequential");

    /**
     * This method implements the createMediator method of the MediatorFactory interface
     * 
     * @param elem - OMElement describing the element which will be parsed
     *  to build the CloneMediator
     * @param properties
     * @return Mediator of the type CloneMediator built from the config element
     */
    public Mediator createSpecificMediator(OMElement elem, Properties properties) {
    	
    	boolean asynchronousExe = true;
        
    	ScatterGather mediator = new ScatterGather();
        processAuditStatus(mediator, elem);

        OMAttribute synchronousExeAttr= elem.getAttribute(SEQUENTIAL_Q);
        if (synchronousExeAttr != null && synchronousExeAttr.getAttributeValue().equals("true")) {
        	asynchronousExe = false;
        }

        mediator.setSequential(!asynchronousExe);
        
        Iterator targetElements = elem.getChildrenWithName(TARGET_Q);
        while (targetElements.hasNext()) {
        	Target target = TargetFactory.createTarget((OMElement)targetElements.next(), properties);
        	target.setAsynchronous(asynchronousExe);
            mediator.addTarget(target);
        }

        OMElement aggregateElement = elem.getFirstChildWithName(
                new QName(SynapseConstants.SYNAPSE_NAMESPACE, "aggregate"));

        OMAttribute aggregateExpr = aggregateElement.getAttribute(ATT_VALUE_TO_AGGREGATE);
        if (aggregateExpr != null) {
            try {
                mediator.setAggregationExpression(
                        SynapsePathFactory.getSynapsePath(aggregateElement, ATT_VALUE_TO_AGGREGATE));
            } catch (JaxenException e) {
                handleException("Unable to load the aggregating XPATH", e);
            }
        }

        OMAttribute conditionExpr = aggregateElement.getAttribute(ATT_CONDITION);
        if (conditionExpr != null) {
            try {
                mediator.setCorrelateExpression(
                        SynapsePathFactory.getSynapsePath(aggregateElement, ATT_CONDITION));
            } catch (JaxenException e) {
                handleException("Unable to load the aggregating XPATH", e);
            }
        }

        OMAttribute completeTimeout = aggregateElement.getAttribute(ATT_TIMEOUT);
        if (completeTimeout != null) {
            mediator.setCompletionTimeoutMillis(Long.parseLong(completeTimeout.getAttributeValue()) * 1000);
        }

        OMAttribute targetVariable = aggregateElement.getAttribute(ATT_TARGET_VARIABLE);
        if (completeTimeout != null) {
            mediator.setTargetVariable(targetVariable.getAttributeValue());
        }

        OMAttribute minMessages = aggregateElement.getAttribute(ATT_MIN_MESSAGES);
        if (minMessages != null) {
            mediator.setMinMessagesToComplete(new ValueFactory().createValue("min-messages", aggregateElement));
        }

        OMAttribute maxMessages = aggregateElement.getAttribute(ATT_MAX_MESSAGES);
        if (maxMessages != null) {
            mediator.setMaxMessagesToComplete(new ValueFactory().createValue("max-messages", aggregateElement));
        }

        addAllCommentChildrenToList(elem, mediator.getCommentsList());
        return mediator;
    }

    /**
     * This method will implement the getTagQName method of the MediatorFactory interface
     *
     * @return QName of the clone element in xml configuration
     */
    public QName getTagQName() {
        return SCATTER_GATHER_Q;
    }
}
