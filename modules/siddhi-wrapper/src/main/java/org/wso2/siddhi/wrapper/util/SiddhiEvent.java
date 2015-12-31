package org.wso2.siddhi.wrapper.util;


public class SiddhiEvent {

    private String streamId;
    private Object[] eventObject;

    public SiddhiEvent(String streamId, Object[] eventObject) {
        this.streamId = streamId;
        this.eventObject = eventObject;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public Object[] getEventObject() {
        return eventObject;
    }

    public void setEventObject(Object[] eventObject) {
        this.eventObject = eventObject;
    }
}
