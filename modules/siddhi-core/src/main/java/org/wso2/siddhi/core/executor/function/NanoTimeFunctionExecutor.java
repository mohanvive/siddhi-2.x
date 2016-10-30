package org.wso2.siddhi.core.executor.function;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.query.api.definition.Attribute;


public class NanoTimeFunctionExecutor extends FunctionExecutor {

    @Override
    public void init(Attribute.Type[] attributeTypes, SiddhiContext siddhiContext) {
     }

    @Override
    protected Object process(Object data) {
        return System.nanoTime();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.LONG;
    }
}
