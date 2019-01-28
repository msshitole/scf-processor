package com.example.scfprocessor;

import com.prime.avro.document.DocumentImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.annotation.*;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.schema.avro.AvroSchemaMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;
import reactor.core.publisher.Flux;

import java.util.function.Function;

@SpringBootApplication
@EnableBinding(Processor.class)
@ConfigurationProperties
public class ScfProcessorApplication {
    private Resource schemaLocation;

    public void setSchemaLocation(Resource schemaLocation) {
        this.schemaLocation = schemaLocation;
    }

    public static void main(String[] args) {
        SpringApplication.run(ScfProcessorApplication.class,
                args);
    }

    @Bean
    public Function<Flux<DocumentImpl>, Flux<DocumentImpl>> process() {
        return documentFlux -> documentFlux.map(document -> {
            document.setTitle(document.getTitle().toUpperCase());
            return document;
        }).log();
    }

    @Bean
    @StreamMessageConverter
    public MessageConverter hcTwoDocumentMessageConverter() {
        AvroSchemaMessageConverter avroSchemaMessageConverter = new AvroSchemaMessageConverter(
                MimeType.valueOf("avro/bytes"));

        if (schemaLocation != null) {
            System.out.println("Setting schema location...");
            avroSchemaMessageConverter.setSchemaLocation(schemaLocation);
        }
        return avroSchemaMessageConverter;
    }

}

