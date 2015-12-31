/*
* Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy
* of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for the
* specific language governing permissions and limitations under the License.
*/


package org.wso2.siddhi.wrapper;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.util.collection.queue.SiddhiQueue;
import org.wso2.siddhi.wrapper.util.SiddhiEvent;
import org.wso2.siddhi.wrapper.util.SiddhiEventConsumer;

import java.util.*;


public class SiddhiWrapper {

    private SiddhiManager siddhiManager;
    private String[] streamDefinitionArray;
    private String siddhiQuery;
    private SiddhiConfiguration siddhiConfiguration;
    private Map<String, SiddhiManager> dynamicSiddhiManagerImpl = new LinkedHashMap<String, SiddhiManager>();
    private List<SiddhiEventConsumer> siddhiEventConsumerList = new ArrayList<SiddhiEventConsumer>();
    private Logger log = Logger.getLogger(SiddhiWrapper.class);
    private SiddhiQueue<SiddhiEvent> siddhiEventQueue = new SiddhiQueue<SiddhiEvent>();
    private SiddhiQueue<SiddhiQueue<SiddhiEvent>> siddhiEventQueueGroup = new SiddhiQueue<SiddhiQueue<SiddhiEvent>>();
    private SiddhiQueue<SiddhiEvent> currentProcessedEventQueue = new SiddhiQueue<SiddhiEvent>();
    private long lastEventTimeStamp;
    private long withinTimeInterval = 1000;
    private Timer timer;
    ScheduledTask st;

    public void createExecutionPlan(String[] streamDefinitionArray, String siddhiQuery, SiddhiConfiguration siddhiConfiguration) {

        this.streamDefinitionArray = streamDefinitionArray;
        this.siddhiQuery = siddhiQuery;
        this.siddhiConfiguration = siddhiConfiguration;
        spinSiddhiManagerInstance();
    }

    public void registerCallback(SiddhiEventConsumer siddhiEventConsumer) {
        siddhiEventConsumerList.add(siddhiEventConsumer);
    }


    public void spinSiddhiManagerInstance() {

        if (siddhiConfiguration != null) {
            siddhiManager = new SiddhiManager(siddhiConfiguration);
        } else {
            siddhiManager = new SiddhiManager();
        }

        for (String streamDefinition : streamDefinitionArray) {
            siddhiManager.defineStream(streamDefinition);
        }

        String queryReference = siddhiManager.addQuery(siddhiQuery);
        dynamicSiddhiManagerImpl.put(queryReference, siddhiManager);
        (new Thread(new EventConsumer(siddhiManager, queryReference))).start();
        (new Thread(new EventPublisher(siddhiManager))).start();
    }

    public void shutdown() {
        siddhiManager.shutdown();
        for (SiddhiManager siddhiManager : dynamicSiddhiManagerImpl.values()) {
            siddhiManager.shutdown();
        }
    }

    public void sentEvents(String streamId, Object[] event) {

        if (currentProcessedEventQueue.size() == 0) {
            lastEventTimeStamp = System.currentTimeMillis() + withinTimeInterval;
            currentProcessedEventQueue.put(new SiddhiEvent(streamId, event));
            System.out.println("entered*****");
            timer = new Timer();
            st = new ScheduledTask();
            timer.schedule(st, withinTimeInterval);


        } else if (System.currentTimeMillis() > lastEventTimeStamp) {
            System.out.println("cancelled");
            timer.cancel();
            siddhiEventQueueGroup.put(currentProcessedEventQueue);
            currentProcessedEventQueue = new SiddhiQueue<SiddhiEvent>();
            currentProcessedEventQueue.put(new SiddhiEvent(streamId, event));
            if (currentProcessedEventQueue.size() > 2) {
                spinSiddhiManagerInstance();
                System.out.println("spin");
            }
            System.out.println("entered & removed 1*****");
        } else {
            currentProcessedEventQueue.put(new SiddhiEvent(streamId, event));
            System.out.println("entered*****");
        }

    }


    class EventConsumer implements Runnable {

        SiddhiManager siddhiManager;
        String queryReference;

        EventConsumer(SiddhiManager siddhiManager, String queryReference) {
            this.siddhiManager = siddhiManager;
            this.queryReference = queryReference;
        }

        @Override
        public void run() {
            siddhiManager.addCallback(queryReference, new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    for (SiddhiEventConsumer siddhiEventConsumer : siddhiEventConsumerList) {
                        siddhiEventConsumer.receiveEvents(timeStamp, inEvents, removeEvents);

                    }
                }
            });
        }
    }

    class EventPublisher implements Runnable {

        SiddhiManager siddhiManager;

        EventPublisher(SiddhiManager siddhiManager){
            this.siddhiManager = siddhiManager;
        }

        @Override
        public void run() {
            while (true) {
                SiddhiQueue<SiddhiEvent> siddhiQueue = siddhiEventQueueGroup.poll();
                if (siddhiQueue != null) {
                    while (siddhiQueue.size() > 0) {
                        SiddhiEvent siddhiEvent = siddhiQueue.poll();
                        if (siddhiEvent != null) {
                            try {
                                System.out.println("POLLED");
                                siddhiManager.getInputHandler(siddhiEvent.getStreamId()).send(siddhiEvent.getEventObject());
                            } catch (InterruptedException e) {
                                log.error("Error while sending events", e);
                            }
                        }
                    }
                }
            }
        }
    }

    class ScheduledTask extends TimerTask {

        public void run() {
            System.out.println("Called" + System.currentTimeMillis());
            if (currentProcessedEventQueue.size() > 0) {
                siddhiEventQueueGroup.put(currentProcessedEventQueue);
                currentProcessedEventQueue = new SiddhiQueue<SiddhiEvent>();
                if (currentProcessedEventQueue.size() > 2) {
                    spinSiddhiManagerInstance();
                    System.out.println("spin");
                }
                System.out.println("entered & removed 2*****");
            }

            timer.cancel();
        }
    }


}
