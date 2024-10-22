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
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.mediators.v2.IfElseMediator;
import org.jaxen.JaxenException;

import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.xml.namespace.QName;

/**
 * <p>Creates a if-else mediator instance with the default behavior</p>
 *
 * <pre>
 * &lt;if-else (valueToCompare="expression" regex="string") | condition="expression"&gt;
 *   mediator+
 * &lt;/if-else&gt;
 * </pre>
 *
 * <p>or if the if-else mediator needs to support the else behavior as well (i.e. a set of mediators
 * to be executed when the if-else evaluates to false).</p>
 *
 * <pre>
 * &lt;if-else (valueToCompare="expression" regex="string") | condition="expression"&gt;
 *   &lt;then&gt;
 *      mediator+
 *   &lt;/then&gt;
 *   &lt;else&gt;
 *      mediator+
 *   &lt;/else&gt;
 * &lt;/if-else&gt;
 * </pre>
 */
public class IfElseMediatorFactory extends AbstractListMediatorFactory {

    protected static final QName ATT_CONDITION = new QName("condition");
    protected static final QName ATT_VALUE_TO_COMPARE = new QName("valueToCompare");
    private static final QName IF_ELSE_Q = new QName(SynapseConstants.SYNAPSE_NAMESPACE, "if-else");
    private static final QName THEN_Q = new QName(SynapseConstants.SYNAPSE_NAMESPACE, "then");
    private static final QName ELSE_Q = new QName(SynapseConstants.SYNAPSE_NAMESPACE, "else");

    public Mediator createSpecificMediator(OMElement elem, Properties properties) {

        IfElseMediator ifElseMediator = new IfElseMediator();

        OMAttribute attCondition = elem.getAttribute(ATT_CONDITION);
        OMAttribute attValueToCompare = elem.getAttribute(ATT_VALUE_TO_COMPARE);
        OMAttribute attRegex = elem.getAttribute(ATT_REGEX);

        if (attCondition != null) {
            if (StringUtils.isNotBlank(attCondition.getAttributeValue())) {
                try {
                    ifElseMediator.setCondition(SynapsePathFactory.getSynapsePath(elem, ATT_CONDITION));
                } catch (JaxenException e) {
                    handleException("Invalid expression for attribute condition : "
                            + attCondition.getAttributeValue(), e);
                }
            } else {
                handleException("Invalid attribute value specified for condition");
            }

        } else if (attValueToCompare != null && attRegex != null) {
            if (StringUtils.isNotBlank(attValueToCompare.getAttributeValue()) &&
                    StringUtils.isNotBlank(attRegex.getAttributeValue())) {
                try {
                    ifElseMediator.setValueToCompare(SynapsePathFactory.getSynapsePath(elem, ATT_VALUE_TO_COMPARE));
                } catch (JaxenException e) {
                    handleException("Invalid expression for attribute valueToCompare : "
                            + attValueToCompare.getAttributeValue(), e);
                }
                try {
                    ifElseMediator.setRegex(Pattern.compile(attRegex.getAttributeValue()));
                } catch (PatternSyntaxException pse) {
                    handleException("Invalid Regular Expression for attribute regex : "
                            + attRegex.getAttributeValue(), pse);
                }
            } else {
                handleException("Invalid attribute values for valueToCompare and/or regex specified");
            }
        } else {
            handleException("An condition or (valueToCompare, regex) attributes are required for a if-else");
        }

        processAuditStatus(ifElseMediator, elem);

        OMElement thenElem = elem.getFirstChildWithName(THEN_Q);

        if (thenElem != null) {

            ifElseMediator.setThenElementPresent(true);
            addChildren(thenElem, ifElseMediator, properties);

            OMElement elseElem = elem.getFirstChildWithName(ELSE_Q);

            if (elseElem != null) {
                AnonymousListMediator listMediator = AnonymousListMediatorFactory
                        .createAnonymousListMediator(elseElem, properties);
                ifElseMediator.setElseMediator(listMediator);
            }

        } else {
            ifElseMediator.setThenElementPresent(false);
            addChildren(elem, ifElseMediator, properties);
        }
        return ifElseMediator;
    }

    public QName getTagQName() {

        return IF_ELSE_Q;
    }
}
