/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com) All Rights Reserved.
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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.util.JavaUtils;
import org.apache.commons.logging.Log;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.SynapseConfigUtils;
import org.apache.synapse.config.xml.XMLConfigConstants;

public class ScatterGatherUtils {

    /**
     * Check whether the message is a scatter message or not
     *
     * @param synCtx MessageContext
     * @return true if the message is a scatter message
     */
    public static boolean isScatterMessage(MessageContext synCtx) {

        Boolean isScatterMessage = (Boolean) synCtx.getProperty(SynapseConstants.SCATTER_MESSAGES);
        return isScatterMessage != null && isScatterMessage;
    }

    /**
     * Check whether the message is a foreach message or not
     *
     * @param synCtx MessageContext
     * @return true if the message is a foreach message
     */
    public static boolean isContinuationTriggeredFromMediatorWorker(MessageContext synCtx) {

        Boolean isContinuationTriggeredMediatorWorker =
                (Boolean) synCtx.getProperty(SynapseConstants.CONTINUE_FLOW_TRIGGERED_FROM_MEDIATOR_WORKER);
        return isContinuationTriggeredMediatorWorker != null && isContinuationTriggeredMediatorWorker;
    }

    public static Object convertValue(String value, String type, Log log) {

        if (type == null) {
            return value;
        }

        try {
            XMLConfigConstants.DATA_TYPES dataType = XMLConfigConstants.DATA_TYPES.valueOf(type);
            switch (dataType) {
                case BOOLEAN:
                    return JavaUtils.isTrueExplicitly(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case FLOAT:
                    return Float.parseFloat(value);
                case INTEGER:
                    return Integer.parseInt(value);
                case LONG:
                    return Long.parseLong(value);
                case OM:
                    return buildOMElement(value);
                case SHORT:
                    return Short.parseShort(value);
                case JSON:
                    return buildJSONElement(value, log);
                default:
                    return value;
            }
        } catch (IllegalArgumentException e) {
            String msg = "Unknown type : " + type + " for the variable mediator or the " +
                    "variable value cannot be converted into the specified type.";
            log.error(msg, e);
            throw new SynapseException(msg, e);
        }
    }


    public static OMElement buildOMElement(String xml) {

        if (xml == null) {
            return null;
        }
        OMElement result = SynapseConfigUtils.stringToOM(xml);
        result.buildWithAttachments();
        return result;
    }

    private static JsonElement buildJSONElement(String jsonPayload, Log log) {

        JsonParser jsonParser = new JsonParser();
        try {
            return jsonParser.parse(jsonPayload);
        } catch (JsonSyntaxException ex) {
            // Enclosing using quotes due to the following issue
            // https://github.com/google/gson/issues/1286
            String enclosed = "\"" + jsonPayload + "\"";
            try {
                return jsonParser.parse(enclosed);
            } catch (JsonSyntaxException e) {
                // log the original exception and discard the new exception
                log.error("Malformed JSON payload : " + jsonPayload, ex);
                return null;
            }
        }
    }
}
