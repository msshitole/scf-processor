package com.example.scfprocessor;

import com.prime.avro.document.DocumentImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.*;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.schema.avro.AvroSchemaMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;
import reactor.core.publisher.Flux;

import java.util.function.Function;

@SpringBootApplication
@EnableBinding(Processor.class)
public class ScfProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ScfProcessorApplication.class,
                "--spring.cloud.stream.function.definition=process");
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
        AvroSchemaMessageConverter converter = new AvroSchemaMessageConverter(MimeType.valueOf("application/vnd.com.prime.avro.document.DocumentImpl.v1+avro"));
        converter.setSchema(DocumentImpl.getClassSchema());
        return converter;
    }

}

