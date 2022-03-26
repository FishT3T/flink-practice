package com.fish.pojo;

import java.sql.Timestamp;

/**
 * @author Yuyi-YuShaoyu
 * @Description
 * @create 2022-03-26 17:00
 * @Modified By
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    public Event() {
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
