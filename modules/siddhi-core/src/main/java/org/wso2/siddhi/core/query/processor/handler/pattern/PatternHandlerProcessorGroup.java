package org.wso2.siddhi.core.query.processor.handler.pattern;


import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.query.QueryPartitioner;
import org.wso2.siddhi.core.query.processor.handler.HandlerProcessor;
import org.wso2.siddhi.core.util.collection.queue.SiddhiQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PatternHandlerProcessorGroup implements HandlerProcessor {

    List<PatternHandlerProcessor> patternHandlerProcessorList = new ArrayList<PatternHandlerProcessor>();
    List<HandlerProcessor> patternHandlerProcessorList2;
    Map<String,SiddhiQueue<StreamEvent>> siddhiEventMap = new HashMap<String,SiddhiQueue<StreamEvent>>();

    private QueryPartitioner queryPartitioner;

    public void receive(String streamID, StreamEvent streamEvent) {

        //siddhiEventMap.get(streamID).put(streamEvent);

        //System.out.println("Inner Receive");
        for (PatternHandlerProcessor patternHandlerProcessor : patternHandlerProcessorList) {
            if (patternHandlerProcessor.getStreamId().equals(streamID)) {
                patternHandlerProcessor.receive(streamEvent);
            }
        }

//        if (patternHandlerProcessorList2 == null) {
//            patternHandlerProcessorList2 = queryPartitioner.constructPartition();
//        }
//
//        for (HandlerProcessor patternHandlerProcessor : patternHandlerProcessorList2) {
//            if (patternHandlerProcessor.getStreamId().equals(streamID)) {
//                patternHandlerProcessor.receive(streamEvent);
//            }
//        }

    }

    public void addPatternHandlerProcessor(PatternHandlerProcessor patternHandlerProcessor) {
        this.patternHandlerProcessorList.add(patternHandlerProcessor);
    }

    public synchronized void setQueryPartition(QueryPartitioner queryPartitioner) {
        this.queryPartitioner = queryPartitioner;
    }

    @Override
    public String getStreamId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void receive(StreamEvent streamEvent) {
        System.out.println("dump receive method");
    }

}
