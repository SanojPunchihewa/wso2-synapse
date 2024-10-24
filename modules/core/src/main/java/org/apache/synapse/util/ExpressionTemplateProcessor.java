package org.apache.synapse.util;

import org.apache.synapse.MessageContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExpressionTemplateProcessor {

    private static final Pattern EXPRESSION_PLACEHOLDER_PATTERN = Pattern.compile("#\\[(.+?)\\]");

    public static String processMessageTemplate(MessageContext synCtx, String template) {
        Matcher matcher = EXPRESSION_PLACEHOLDER_PATTERN.matcher(template);

        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String placeholder = matcher.group(1); // Extract the expression inside #[...]
            String replacement = ExpressionResolver.resolve(placeholder, synCtx);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    // Dummy resolver for expressions to test the processor for Log mediator
    // TODO replace this with actual expression resolver
    static class ExpressionResolver {
        public static String resolve(String expression, MessageContext synCtx) {
            String variableName = expression.substring(4);
            if (synCtx.getVariable(variableName) != null) {
                return synCtx.getVariable(variableName).toString();
            }
            return expression;
        }
    }
}
