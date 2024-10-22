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

import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.mediators.filters.FilterMediator;
import org.apache.synapse.mediators.v2.IfElseMediator;

/**
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
public class IfElseMediatorSerializer extends AbstractListMediatorSerializer {

    public OMElement serializeSpecificMediator(Mediator m) {

        if (!(m instanceof IfElseMediator)) {
            handleException("Unsupported mediator passed in for serialization : " + m.getType());
        }

        IfElseMediator mediator = (IfElseMediator) m;
        OMElement ifElse = fac.createOMElement("if-else", synNS);

        if (mediator.getValueToCompare() != null && mediator.getRegex() != null) {

            SynapsePathSerializer.serializePath(mediator.getValueToCompare(), ifElse, "valueToCompare");

            ifElse.addAttribute(fac.createOMAttribute(
                    "regex", nullNS, mediator.getRegex().pattern()));

        } else if (mediator.getCondition() != null) {

            SynapsePathSerializer.serializePath(mediator.getCondition(), ifElse, "condition");

        } else {
            handleException("Invalid if-else mediator. " +
                    "Should have either a 'valueToCompare' and a 'regex' OR a 'condition'");
        }

        saveTracingState(ifElse, mediator);

        if (mediator.isThenElementPresent()) {
            OMElement thenElem = fac.createOMElement("then", synNS);
            ifElse.addChild(thenElem);
            serializeChildren(thenElem, mediator.getList());

            if (mediator.getElseMediator() != null) {

                OMElement elseElem = fac.createOMElement("else", synNS);
                ifElse.addChild(elseElem);
                serializeChildren(elseElem, mediator.getElseMediator().getList());
            }
        } else {
            serializeChildren(ifElse, mediator.getList());
        }
        return ifElse;
    }

    public String getMediatorClassName() {

        return FilterMediator.class.getName();
    }
}
