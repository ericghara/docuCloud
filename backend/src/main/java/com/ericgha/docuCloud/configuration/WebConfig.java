package com.ericgha.docuCloud.configuration;

import com.ericgha.docuCloud.converter.CsvLtreeFormatter;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;

@Configuration
@EnableWebFlux
public class WebConfig implements WebFluxConfigurer {

    @Override
    public void addFormatters(FormatterRegistry registry) {
        CsvLtreeFormatter formatter = new CsvLtreeFormatter();
        // todo: if this doesn't work look into registrar
        registry.addFormatter( formatter );
    }
}
