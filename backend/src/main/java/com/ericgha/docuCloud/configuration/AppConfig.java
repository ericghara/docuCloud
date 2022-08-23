package com.ericgha.docuCloud.configuration;

import com.ericgha.docuCloud.service.AppEnvPropertiesService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@RequiredArgsConstructor
public class AppConfig {

    private final Environment env;

    @Bean
    AppEnvPropertiesService propertiesService() {
        return new AppEnvPropertiesService( env );
    }
}
