package com.ericgha.docuCloud.configuration;

import com.ericgha.docuCloud.converter.LtreeFormatter;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;

@Configuration
@EnableWebFlux
public class WebConfig implements WebFluxConfigurer {

    @Override
    public void addFormatters(FormatterRegistry registry) {
        LtreeFormatter formatter = new LtreeFormatter();
        // todo: if this doesn't work look into registrar
        registry.addFormatter( formatter );
    }
}
