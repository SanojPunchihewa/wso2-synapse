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
import org.apache.synapse.Mediator;
import org.apache.synapse.SynapseException;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.builtin.PropertyMediator;
import org.apache.synapse.mediators.v2.VariableMediator;
import org.apache.synapse.util.MediatorPropertyUtils;
import org.apache.synapse.util.xpath.SynapseJsonPath;
import org.apache.synapse.util.xpath.SynapseXPath;
import org.jaxen.JaxenException;

import java.util.Properties;
import java.util.regex.Pattern;
import javax.xml.namespace.QName;

/**
 * Creates a variable mediator through the supplied XML configuration
 * <p/>
 * <pre>
 * &lt;variable name="string" [action=set/remove] value="string|expression" type="string|integer|JSON"/&gt;
 * </pre>
 */
public class VariableMediatorFactory extends AbstractMediatorFactory {

    private static final QName ATT_ACTION = new QName("action");
    private static final QName ATT_TYPE = new QName("type");

    public Mediator createSpecificMediator(OMElement elem, Properties properties) {

        VariableMediator variableMediator = new VariableMediator();
        OMAttribute name = elem.getAttribute(ATT_NAME);
        OMAttribute value = elem.getAttribute(ATT_VALUE);
        OMAttribute action = elem.getAttribute(ATT_ACTION);
        OMAttribute type = elem.getAttribute(ATT_TYPE);

        OMElement valueElement = elem.getFirstElement();

        if (name == null || name.getAttributeValue().isEmpty()) {
            String msg = "The 'name' attribute is required for the configuration of a variable mediator";
            log.error(msg);
            throw new SynapseException(msg);
        } else if ((value == null && !(action != null && "remove".equals(action.getAttributeValue())))) {
            String msg = "The 'value' attributes is required for a variable mediator when action is SET";
            log.error(msg);
            throw new SynapseException(msg);
        }

//        //check the property name dynamic or not
//        String nameAttributeValue = name.getAttributeValue();
//        if (MediatorPropertyUtils.isDynamicName(nameAttributeValue)) {
//            try {
//                String nameExpression = nameAttributeValue.substring(1, nameAttributeValue.length() - 1);
//                if(nameExpression.startsWith("json-eval(")) {
//                    new SynapseJsonPath(nameExpression.substring(10, nameExpression.length() - 1));
//                } else {
//                    new SynapseXPath(nameExpression);
//                }
//            } catch (JaxenException e) {
//                String msg = "Invalid expression for attribute 'name' : " + nameAttributeValue;
//                log.error(msg);
//                throw new SynapseException(msg);
//            }
//            // ValueFactory for creating dynamic Value
//            ValueFactory nameValueFactory = new ValueFactory();
//            // create dynamic Value based on OMElement
//            Value generatedNameValue = nameValueFactory.createValue(XMLConfigConstants.NAME, elem);
//            variableMediator.setDynamicNameValue(generatedNameValue);
//        }

        String dataType = null;
        if (type != null) {
            dataType = type.getAttributeValue();
        }

        variableMediator.setName(name.getAttributeValue());
        variableMediator.setValue(value.getAttributeValue(), dataType);

        // The action attribute is optional, if provided and equals to 'remove' the
        // variable mediator will remove the variable
        if (action != null && "remove".equals(action.getAttributeValue())) {
            variableMediator.setAction(PropertyMediator.ACTION_REMOVE);
        }

        // after successfully creating the mediator
        // set its common attributes such as tracing etc
        processAuditStatus(variableMediator, elem);

        addAllCommentChildrenToList(elem, variableMediator.getCommentsList());

        return variableMediator;
    }

    public QName getTagQName() {
        return PROP_Q;
    }

}
