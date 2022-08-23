package com.ericgha.docuCloud.service;

public interface PropertyKey {

    /**
     *
     * @return full configuration key e.g. {@code spring.r2dbc.username}
     */
    public String get();

}
