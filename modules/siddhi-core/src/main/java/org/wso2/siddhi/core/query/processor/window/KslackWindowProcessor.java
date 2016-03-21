/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.siddhi.core.query.processor.window;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.ListEvent;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.query.QueryPostProcessingElement;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;

import java.util.*;

/**
 * The following code conducts reordering of an out-of-order event stream.
 * This implements the K-Slack based disorder handling algorithm which was originally described in
 * https://www2.informatik.uni-erlangen.de/publication/download/IPDPS2013.pdf
 */

public class KslackWindowProcessor extends WindowProcessor {

    private long k = 0; //In the beginning the K is zero.
    private long greatestTimestamp = 0; //Used to track the greatest timestamp of tuples seen so far in the stream history.
    private TreeMap<Long, HashSet<InEvent>> eventTreeMap;
    private TreeMap<Long, HashSet<InEvent>> expiredEventTreeMap;
    private int timeStampAttributePosition;
    private long MAX_K = Long.MAX_VALUE;
    private KSlackTimerTask timerTask;
    private Timer timer;
    private long TIMER_DURATION = -1l;
    private boolean expireFlag = false;
    private long lastSentTimeStamp = -1l;


    @Override
    protected void processEvent(InEvent event) {
        acquireLock();
        try {
            processDuplicateEvent(event);
        } finally {
            releaseLock();
        }
    }

    @Override
    protected void processEvent(InListEvent listEvent) {
        acquireLock();
        try {
            Event[] events = listEvent.getEvents();
            for (int i = 0, events1Length = listEvent.getActiveEvents(); i < events1Length; i++) {
                processDuplicateEvent((InEvent) events[i]);
            }
        } finally {
            releaseLock();
        }
    }

    @Override
    public Iterator<StreamEvent> iterator() {
        //nothing
        System.out.println("Iterator called");
        return null;
    }

    @Override
    public Iterator<StreamEvent> iterator(String predicate) {
        //nothing
        System.out.println("Iterator predicate called");
        return null;
    }

    private void processDuplicateEvent(InEvent event) {

        List<InEvent> newEventList = new ArrayList<InEvent>();
        try {
            long timestamp = (Long) event.getData(timeStampAttributePosition);
            if (expireFlag) {
                if (timestamp < lastSentTimeStamp) {
                    return;
                }
            }
            HashSet<InEvent> eventList = eventTreeMap.get(timestamp);
            if (eventList == null) {
                eventList = new HashSet<InEvent>();
            }
            eventList.add(event);
            eventTreeMap.put(timestamp, eventList);

            if (timestamp > greatestTimestamp) {

                greatestTimestamp = timestamp;
                long minTimestamp = eventTreeMap.firstKey();

                if ((greatestTimestamp - minTimestamp) > k) {
                    if ((greatestTimestamp - minTimestamp) < MAX_K) {
                        k = greatestTimestamp - minTimestamp;
                    } else {
                        k = MAX_K;
                    }
                }

                Iterator<Map.Entry<Long, HashSet<InEvent>>> entryIterator = eventTreeMap.entrySet().iterator();
                while (entryIterator.hasNext()) {
                    Map.Entry<Long, HashSet<InEvent>> entry = entryIterator.next();
                    HashSet<InEvent> list = expiredEventTreeMap.get(entry.getKey());

                    if (list != null) {
                        list.addAll(entry.getValue());
                    } else {
                        expiredEventTreeMap.put(entry.getKey(), entry.getValue());
                    }
                }
                eventTreeMap = new TreeMap<Long, HashSet<InEvent>>();

                entryIterator = expiredEventTreeMap.entrySet().iterator();
                while (entryIterator.hasNext()) {
                    Map.Entry<Long, HashSet<InEvent>> entry = entryIterator.next();

                    if (entry.getKey() + k <= greatestTimestamp) {
                        entryIterator.remove();
                        HashSet<InEvent> timeEventList = entry.getValue();
                        lastSentTimeStamp = entry.getKey();

                        for (InEvent aTimeEventList : timeEventList) {
                            newEventList.add(aTimeEventList);
                        }
                    }
                }
            }

        } catch (ArrayIndexOutOfBoundsException ec) {
            //This happens due to user specifying an invalid field index.
            throw new QueryCreationException("The very first parameter must be an Integer with a valid " +
                    " field index (0 to (fieldsLength-1)).");
        }

        InEvent[] inEvents = newEventList.toArray(new InEvent[newEventList.size()]);
        nextProcessor.process(new InListEvent(inEvents));
        if (TIMER_DURATION != -1l) {
            timerTask.setInfo(expiredEventTreeMap, nextProcessor);
        }

    }

    @Override
    protected Object[] currentState() {
        return new Object[0];
    }

    @Override
    protected void restoreState(Object[] data) {
        //Do nothing
    }

    @Override
    protected void init(Expression[] parameters, QueryPostProcessingElement nextProcessor, AbstractDefinition streamDefinition, String elementId, boolean async, SiddhiContext siddhiContext) {

        if (parameters.length > 2) {
            throw new QueryCreationException("Maximum four input parameters can be specified for KSlack. " +
                    " Timestamp field (long), k-slack buffer expiration time-out window (long), Max_K size (long), and boolean " +
                    " flag to indicate whether the late events should get discarded. But found " +
                    parameters.length + " attributes.");
        }

        //This is the most basic case. Here we do not use a timer. The basic K-slack algorithm is implemented.
        String timeStampAttributeName;
        if (parameters.length == 1) {
            timeStampAttributeName = ((Variable) parameters[0]).getAttributeName();
            timeStampAttributePosition = definition.getAttributePosition(timeStampAttributeName);
            //In the following case we have the timer operating in background. But we do not impose a K-slack window length.
        } else if (parameters.length == 2) {
            timeStampAttributeName = ((Variable) parameters[0]).getAttributeName();
            timeStampAttributePosition = definition.getAttributePosition(timeStampAttributeName);
            TIMER_DURATION = ((IntConstant) parameters[1]).getValue();
            //In the third case we have both the timer operating in the background and we have also specified a K-slack window length.
        }

        eventTreeMap = new TreeMap<Long, HashSet<InEvent>>();
        expiredEventTreeMap = new TreeMap<Long, HashSet<InEvent>>();

        if (TIMER_DURATION != -1l) {
            timer = new Timer();
            timerTask = new KSlackTimerTask();
            timer.schedule(timerTask, 0, TIMER_DURATION);
        }
    }

    @Override
    public void destroy() {

    }

    class KSlackTimerTask extends TimerTask {
        private TreeMap<Long, HashSet<InEvent>> expiredEventTreeMap;
        private QueryPostProcessingElement nextProcessor;

        public void setInfo(TreeMap<Long, HashSet<InEvent>> treeMap, QueryPostProcessingElement np) {
            expiredEventTreeMap = treeMap;
            nextProcessor = np;
        }

        @Override
        public void run() {
            if ((expiredEventTreeMap != null) && (expiredEventTreeMap.keySet().size() != 0)) {
                Iterator<Map.Entry<Long, HashSet<InEvent>>> entryIterator = expiredEventTreeMap.entrySet().iterator();
                ListEvent listEvent = new InListEvent();

                while (entryIterator.hasNext()) {
                    Map.Entry<Long, HashSet<InEvent>> entry = entryIterator.next();
                    entryIterator.remove();
                    HashSet<InEvent> timeEventList = entry.getValue();

                    for (InEvent aTimeEventList : timeEventList) {
                        listEvent.addEvent(aTimeEventList);
                    }
                }

                nextProcessor.process(listEvent);
            }
        }
    }

}
