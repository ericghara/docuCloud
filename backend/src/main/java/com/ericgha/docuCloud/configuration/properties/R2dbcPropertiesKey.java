package com.ericgha.docuCloud.configuration.properties;

import com.ericgha.docuCloud.service.PropertyKey;

public enum R2dbcPropertiesKey implements PropertyKey {

    NAME("name"),
    HOST("host"),
    PORT("port"),
    USERNAME("username"),
    PASSWORD("password");

    public static final String PREFIX = "spring.r2dbc";
    public final String key;
    R2dbcPropertiesKey(String key) {
        this.key = key;
    }

    public String get() {
        return String.format("%s.%s", PREFIX, key);
    }

}
