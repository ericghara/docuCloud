package com.ericgha.docuCloud.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatusCodeMapperTest {

    @Test
    @DisplayName("mapThrowable maps correctly to codeMap")
    void mapThrowableMapsCorrectly() {
        IllegalArgumentException e = new IllegalArgumentException();
        Map<Class<? extends Throwable>, HttpStatus> codeMap = Map.of(IllegalArgumentException.class, HttpStatus.BAD_REQUEST);
        HttpStatus found = StatusCodeMapper.mapThrowable(e, codeMap, HttpStatus.INTERNAL_SERVER_ERROR);
        assertEquals(HttpStatus.BAD_REQUEST, found);
    }

    @Test
    @DisplayName("mapThrowable falls through to correct status code")
    void mapThrowableFallsThrough() {
        IllegalArgumentException e = new IllegalArgumentException();
        Map<Class<? extends Throwable>, HttpStatus> codeMap = Map.of(RuntimeException.class, HttpStatus.BAD_REQUEST);
        HttpStatus found = StatusCodeMapper.mapThrowable(e, codeMap, HttpStatus.INTERNAL_SERVER_ERROR);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, found);
    }
}