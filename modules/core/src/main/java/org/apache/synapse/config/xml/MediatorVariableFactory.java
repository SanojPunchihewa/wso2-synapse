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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.Mediator;
import org.apache.synapse.SynapseException;
import org.apache.synapse.mediators.MediatorProperty;
import org.apache.synapse.mediators.MediatorVariable;
import org.jaxen.JaxenException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A utility class capable of creating instances of MediatorVariable objects by reading
 * through a given XML configuration
 *
 * <pre>
 * &lt;element&gt;
 *    &lt;variable name="string" (value="literal" | expression="expression")/&gt;*
 * &lt;/element&gt;
 * </pre>
 */
public class MediatorVariableFactory {

    private static final Log log = LogFactory.getLog(MediatorVariableFactory.class);

    public static List<MediatorVariable> getMediatorVariables(OMElement elem, Mediator mediator) {

        List<MediatorVariable> variableList = new ArrayList<>();

        Iterator variableElements = elem.getChildrenWithName(MediatorVariable.VARIABLE_Q);

        while (variableElements.hasNext()) {

            OMElement variableElement = (OMElement) variableElements.next();
            OMAttribute attName = variableElement.getAttribute(MediatorVariable.ATT_NAME_Q);
            OMAttribute attValue = variableElement.getAttribute(MediatorVariable.ATT_VALUE_Q);
            OMAttribute attExpr = variableElement.getAttribute(MediatorVariable.ATT_EXPR_Q);

            MediatorVariable variable = new MediatorVariable();

            if (attName == null || StringUtils.isBlank(attName.getAttributeValue())) {
                String msg = "Attribute 'name' is a required attribute for a variable";
                log.error(msg);
                throw new SynapseException(msg);
            } else {
                variable.setName(attName.getAttributeValue());
            }

            if (attValue != null) {
                // class mediator can have empty values for value attribute
                if (mediator == null && StringUtils.isBlank(attValue.getAttributeValue())) {

                    String msg = "Entry attribute value (if specified) " +
                            "is required for a Log property";
                    log.error(msg);
                    throw new SynapseException(msg);

                } else {
                    String attributeValue = attValue.getAttributeValue();
                    variable.setValue(attributeValue);
                    if (mediator != null) {
                        // TODO Set variables for class mediators
//                        PropertyHelper.setInstanceProperty(attName.getAttributeValue(), attributeValue, mediator);
                    }
                }

            } else if (attExpr != null) {
                if (StringUtils.isBlank(attExpr.getAttributeValue())) {
                    String msg = "Attribute 'expression' (if specified) is required for a mediator variable";
                    log.error(msg);
                    throw new SynapseException(msg);

                } else {
                    try {
                        variable.setExpression(SynapsePathFactory.getSynapsePath(
                                variableElement, MediatorProperty.ATT_EXPR_Q));
                    } catch (JaxenException e) {
                        String msg = "Invalid expression : " + attExpr.getAttributeValue();
                        log.error(msg);
                        throw new SynapseException(msg, e);
                    }
                }
            } else {
                String msg = "Attribute 'value' OR 'expression' must be specified for a mediator variable";
                log.error(msg);
                throw new SynapseException(msg);
            }
            variableList.add(variable);
        }
        return variableList;
    }

    public static List<MediatorVariable> getMediatorProperties(OMElement elem) {

        return getMediatorVariables(elem, null);
    }
}
