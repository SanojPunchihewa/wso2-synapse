package org.apache.synapse.mediators.v2;

import org.apache.synapse.mediators.AbstractMediator;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class AbstractClassMediator extends AbstractMediator {

    public boolean mediate(org.apache.synapse.MessageContext messageContext) {

        // User implementation method will be called here using VSCode CAR builder
        return true;
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface Arg {

        String name();
        ArgumentType type();
    }

    public enum ArgumentType {
        STRING("String"),
        INTEGER("Integer"),
        BOOLEAN("Boolean");

        private final String typeName;

        ArgumentType(String typeName) {

            this.typeName = typeName;
        }

        public String getTypeName() {

            return typeName;
        }

        @Override
        public String toString() {

            return typeName;
        }
    }
}
