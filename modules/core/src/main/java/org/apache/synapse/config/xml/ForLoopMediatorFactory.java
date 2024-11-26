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

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.commons.lang3.StringUtils;
import org.apache.synapse.Mediator;
import org.apache.synapse.SynapseException;
import org.apache.synapse.mediators.eip.Target;
import org.apache.synapse.mediators.v2.ForEachMediator;
import org.apache.synapse.mediators.v2.ScatterGather;
import org.jaxen.JaxenException;

import java.util.Properties;
import javax.xml.namespace.QName;

/**
 * The &lt;scatter-gather&gt; mediator is used to copy messages in Synapse to similar messages but with
 * different message contexts and aggregate the responses back.
 *
 * <pre>
 * &lt;scatter-gather parallel-execution=(true | false) result-target=(body | variable) content-type=(JSON | XML)&gt;
 *   &lt;aggregation value="expression" condition="expression" timeout="long"
 *     min-messages="expression" max-messages="expression"/&gt;
 *   &lt;sequence&gt;
 *     (mediator)+
 *   &lt;/sequence&gt;+
 * &lt;/scatter-gather&gt;
 * </pre>
 */
public class ForLoopMediatorFactory extends AbstractMediatorFactory {

    /**
     * This will hold the QName of the clone mediator element in the xml configuration
     */
    private static final QName SCATTER_GATHER_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "for-loop");
    private static final QName ATT_COLLECTION = new QName("collection");
    private static final QName ATT_TIMEOUT = new QName("timeout");
    private static final QName SEQUENCE_Q = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "sequence");
    private static final QName PARALLEL_EXEC_Q = new QName("parallel-execution");
    private static final QName RESULT_TARGET_Q = new QName("result-target");
    private static final QName CONTENT_TYPE_Q = new QName("content-type");

    private static final SequenceMediatorFactory fac = new SequenceMediatorFactory();

    public Mediator createSpecificMediator(OMElement elem, Properties properties) {

        boolean asynchronousExe = true;

        ForEachMediator mediator = new ForEachMediator();
        processAuditStatus(mediator, elem);

        OMAttribute parallelExecAttr = elem.getAttribute(PARALLEL_EXEC_Q);
        if (parallelExecAttr != null && parallelExecAttr.getAttributeValue().equals("false")) {
            asynchronousExe = false;
        }
        mediator.setParallelExecution(asynchronousExe);

        OMAttribute contentTypeAttr = elem.getAttribute(CONTENT_TYPE_Q);
        if (contentTypeAttr == null || StringUtils.isBlank(contentTypeAttr.getAttributeValue())) {
            String msg = "The 'content-type' attribute is required for the configuration of a Scatter Gather mediator";
            throw new SynapseException(msg);
        } else {
            if ("JSON".equals(contentTypeAttr.getAttributeValue())) {
                mediator.setContentType(ScatterGather.JSON_TYPE);
            } else if ("XML".equals(contentTypeAttr.getAttributeValue())) {
                mediator.setContentType(ScatterGather.XML_TYPE);
            } else {
                String msg = "The 'content-type' attribute should be either 'JSON' or 'XML'";
                throw new SynapseException(msg);
            }
        }

        OMAttribute resultTargetAttr = elem.getAttribute(RESULT_TARGET_Q);
        if (resultTargetAttr != null && StringUtils.isNotBlank(resultTargetAttr.getAttributeValue())) {
            mediator.setResultTarget(resultTargetAttr.getAttributeValue());
        }

        OMAttribute collectionAttr = elem.getAttribute(ATT_COLLECTION);
        if (collectionAttr == null || StringUtils.isBlank(collectionAttr.getAttributeValue())) {
            String msg = "The 'collection' attribute is required for the configuration of a Scatter Gather mediator";
            throw new SynapseException(msg);
        } else {
            try {
                mediator.setCollectionExpression(SynapsePathFactory.getSynapsePath(elem, ATT_COLLECTION));
            } catch (JaxenException e) {
                String msg = "Unable to build the ForLoop Mediator. Invalid expression "
                        + collectionAttr.getAttributeValue();
                throw new SynapseException(msg);
            }
        }

        OMElement sequenceElement = elem.getFirstChildWithName(SEQUENCE_Q);
        if (sequenceElement == null) {
            String msg = "A 'sequence' element is required for the configuration of a Scatter Gather mediator";
            throw new SynapseException(msg);
        } else {
            Target target = new Target();
            target.setSequence(fac.createAnonymousSequence(sequenceElement, properties));
            target.setAsynchronous(asynchronousExe);
            mediator.setTarget(target);
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
