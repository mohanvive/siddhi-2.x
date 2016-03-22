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

package org.wso2.siddhi.test.standard;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class KslackTestClass {

    static final Logger log = Logger.getLogger(KslackTestClass.class);
    private int count;
    private long value;
    private boolean eventArrived;
    long startingTime = 0;
    long endingTime = 0;
    InputHandler secondHandler;

    @Before
    public void init() {
        count = 0;
        value = 0;
        eventArrived = false;
        startingTime = 0;
        endingTime = 0;
    }


    @Test
    public void perfTest() throws InterruptedException {
        log.info("Window PerfTest");
        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.defineStream("define stream inputStream (eventtt long, price long, volume long); ");

        String queryReference = siddhiManager.addQuery("from inputStream#window.kslack(eventtt , 5000) select eventtt, price, volume " +
                " insert into outputStream; ");

        //secondHandler = sendEvent();

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (org.wso2.siddhi.core.event.Event event : inEvents) {
                    count++;

                    if (count == 1) {
                        org.junit.Assert.assertEquals(1l, event.getData()[0]);
                    }

                    if (count == 2) {
                        org.junit.Assert.assertEquals(4l, event.getData()[0]);
                    }

                    if (count == 3) {
                        org.junit.Assert.assertEquals(3l, event.getData()[0]);
                    }

                    if (count == 4) {
                        org.junit.Assert.assertEquals(5l, event.getData()[0]);
                    }

                    if (count == 5) {
                        org.junit.Assert.assertEquals(6l, event.getData()[0]);
                    }

                    if (count == 6) {
                        org.junit.Assert.assertEquals(7l, event.getData()[0]);
                    }

                    if (count == 7) {
                        org.junit.Assert.assertEquals(8l, event.getData()[0]);
                    }

                    if (count == 8) {
                        org.junit.Assert.assertEquals(9l, event.getData()[0]);
                    }

                    if (count == 9) {
                        org.junit.Assert.assertEquals(10l, event.getData()[0]);
                    }

                    if (count == 10) {
                        org.junit.Assert.assertEquals(13l, event.getData()[0]);
                    }

//                    try {
//                        secondHandler.send(event);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                    }
                }
            }
        });

        InputHandler inputHandler = siddhiManager.getInputHandler("inputStream");

        inputHandler.send(new Object[]{1l, 700f, 100l});
        inputHandler.send(new Object[]{4l, 60.5f, 200l});
        inputHandler.send(new Object[]{3l, 60.5f, 200l});
        inputHandler.send(new Object[]{5l, 700f, 100l});
        inputHandler.send(new Object[]{6l, 60.5f, 200l});
        inputHandler.send(new Object[]{9l, 60.5f, 200l});
        inputHandler.send(new Object[]{7l, 700f, 100l});
        inputHandler.send(new Object[]{8l, 60.5f, 200l});
        inputHandler.send(new Object[]{10l, 60.5f, 200l});
        inputHandler.send(new Object[]{13l, 60.5f, 200l});

        Thread.sleep(7000);
        org.junit.Assert.assertEquals("Event count", 9, count);
        siddhiManager.shutdown();
    }


    private InputHandler sendEvent(){

        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.defineStream("define stream inputStream (eventtt long, price long, volume long); ");

        String queryReference = siddhiManager.addQuery("from inputStream select eventtt, price, volume " +
                " insert into outputStream; ");

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
            }
        });

        InputHandler inputHandler = siddhiManager.getInputHandler("inputStream");
        return inputHandler;

    }


}
