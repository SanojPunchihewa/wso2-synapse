package org.apache.synapse.mediators.v2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.config.xml.SynapsePath;

public class Argument {

    private static final Log log = LogFactory.getLog(Argument.class);
    private SynapsePath expression = null;
    private Object value = null;
    private String type = null;

    public Object getResolvedArgument(MessageContext synCtx) {

        if (value != null) {
            return value;
        } else {
            if (expression != null) {
                return expression.objectValueOf(synCtx);
//                if (isOMType(type)) {
//                    return buildOMElement(expression.stringValueOf(synCtx));
//                } else if (isStringType(type)) {
//                    return expression.stringValueOf(synCtx);
//                }
//                return convertExpressionResult(expression.objectValueOf(synCtx), type);
            }
        }
        return null;
    }

    public void setExpression(SynapsePath expression, String type) {

        this.expression = expression;
        this.type = type;
    }

    public void setValue(String value, String type) {

        this.type = type;
        this.value = ScatterGatherUtils.convertValue(value, type, log);
    }
}
