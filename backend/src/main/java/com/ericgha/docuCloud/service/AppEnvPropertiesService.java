package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.configuration.properties.AwsPropertiesKey;
import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Objects;

@RequiredArgsConstructor
public class AppEnvPropertiesService {

    private final Environment env;

    public <T extends PropertyKey> String get(T key) {
        return Objects.requireNonNull(env.getProperty( key.get() ),
                "Couldn't locate Property: " + key.get() );
    }

}
