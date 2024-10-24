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

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.mediators.MediatorVariable;

import java.util.Collection;
import javax.xml.namespace.QName;

/**
 * A utility class for serializing instances of MediatorVariable objects to XML configuration.
 *
 * <pre>
 * &lt;element&gt;
 *    &lt;variable name="string" (value="literal" | expression="expression")/&gt;*
 * &lt;/element&gt;
 * </pre>
 */
public class MediatorVariableSerializer {

    protected static final OMFactory fac = OMAbstractFactory.getOMFactory();
    protected static final OMNamespace synNS = SynapseConstants.SYNAPSE_OMNAMESPACE;
    protected static final OMNamespace nullNS
            = fac.createOMNamespace(XMLConfigConstants.NULL_NAMESPACE, "");
    protected static final QName VARIABLE_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "variable");
    private static final Log log = LogFactory.getLog(MediatorVariableSerializer.class);

    /**
     * Serialize all the variables to the given paren element. For each and every
     * variable there will be a separate variable element created inside the parent element.
     *
     * @param parent    element to which variable elements should be added
     * @param variables Collection of variables
     */
    public static void serializeMediatorVariables(OMElement parent,
                                                  Collection<MediatorVariable> variables) {

        serializeMediatorVariables(parent, variables, VARIABLE_Q);
    }

    /**
     * Serialize all the variables to the given paren element. For each and every
     * property ther will be a seperate element with the given name created inside the
     * parent element.
     *
     * @param parent           element to which property elements should be added
     * @param props            <code>Collection</code> of propertis
     * @param childElementName <code>QNmae</code> of the property element to be created
     */
    public static void serializeMediatorVariables(OMElement parent, Collection<MediatorVariable> variables,
                                                  QName childElementName) {

        for (MediatorVariable variable : variables) {
            serializeMediatorVariable(parent, variable, childElementName);
        }
    }

    /**
     * Serialize the property to the given paren element. There will be a element created with
     * the name property inside the parent element.
     *
     * @param parent element to which property elements should be added
     * @param mp     a property to be serialized
     */
    public static void serializeMediatorVariable(OMElement parent, MediatorVariable mp) {

        serializeMediatorVariable(parent, mp, VARIABLE_Q);
    }

    /**
     * Serialize the property to the given paren element. There will be a element created with
     * given name inside the parent element.
     *
     * @param parent           element to which property elements should be added
     * @param mp               a property to be serialized
     * @param childElementName <code>QName</code> of the element to be created
     */
    public static void serializeMediatorVariable(OMElement parent, MediatorVariable variable, QName childElementName) {

        OMElement variableElement = fac.createOMElement(childElementName, parent);
        if (variable.getName() != null) {
            variableElement.addAttribute(fac.createOMAttribute("name", nullNS, variable.getName()));
        } else {
            String msg = "Mediator variable name is missing";
            log.error(msg);
            throw new SynapseException(msg);
        }
        if (variable.getValue() != null) {
            variableElement.addAttribute(fac.createOMAttribute("value", nullNS, variable.getValue()));
        } else if (variable.getExpression() != null) {
            SynapsePathSerializer.serializePath(variable.getExpression(), variableElement, "expression");
        } else {
            String msg = "Mediator variable must have a 'value' or an 'expression'";
            log.error(msg);
            throw new SynapseException(msg);
        }
    }
}
