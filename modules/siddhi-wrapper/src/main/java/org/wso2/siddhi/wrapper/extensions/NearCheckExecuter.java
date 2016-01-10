package org.wso2.siddhi.wrapper.extensions;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.AtomicEvent;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

@SiddhiExtension(namespace = "debs", function = "getDistance")
public class NearCheckExecuter extends FunctionExecutor {

    @Override
    public Type getReturnType() {
        return Type.DOUBLE;
    }

    public Object execute(AtomicEvent event) {
        double[] values = new double[attributeExpressionExecutors.size()];
        int index = 0;
        for (ExpressionExecutor executor : attributeExpressionExecutors) {
            values[index] = (Double) executor.execute(event);
            index++;
        }
        return Math.sqrt(Math.pow(values[0] - values[3], 2)
                + Math.pow(values[1] - values[4], 2) + Math.pow(values[2] - values[5], 2));
    }


    @Override
    protected Object process(Object data) {
        System.out.println("Invalid - Process");
        return null;
    }

    @Override
    public void init(Type[] attributeTypes, SiddhiContext siddhiContext) {

    }

    @Override
    public void destroy() {

    }
}


