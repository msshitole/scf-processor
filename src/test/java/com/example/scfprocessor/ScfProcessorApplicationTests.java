package com.example.scfprocessor;

import com.prime.avro.document.DocumentImpl;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.schema.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ScfProcessorApplicationTests {

    @Test
    public void contextLoads() {
    }

    @Test
    public void testSendMessageWithLocation() throws Exception {
        ConfigurableApplicationContext sourceContext = SpringApplication.run(AvroSourceApplication.class,
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--schemaLocation=classpath:schemas/document_v1.schema",
                "--spring.cloud.stream.schemaRegistryClient.enabled=false",
                "--spring.cloud.stream.bindings.output.contentType=avro/bytes");
        Source source = sourceContext.getBean(Source.class);
        DocumentImpl firstOutboundFoo = new DocumentImpl();
        firstOutboundFoo.setId$1(UUID.randomUUID().toString());
        firstOutboundFoo.setTitle("foo" + UUID.randomUUID().toString());
        source.output().send(MessageBuilder.withPayload(firstOutboundFoo).build());
        MessageCollector sourceMessageCollector = sourceContext.getBean(MessageCollector.class);
        Message<?> outboundMessage = sourceMessageCollector.forChannel(source.output()).poll(1000,
                TimeUnit.MILLISECONDS);

        ConfigurableApplicationContext barSourceContext = SpringApplication.run(AvroSourceApplication.class,
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--schemaLocation=classpath:schemas/document_v1.schema",
                "--spring.cloud.stream.schemaRegistryClient.enabled=false",
                "--spring.cloud.stream.bindings.output.contentType=avro/bytes");
        Source barSource = barSourceContext.getBean(Source.class);
        DocumentImpl firstOutboundUser2 = new DocumentImpl();
        firstOutboundUser2.setId$1(UUID.randomUUID().toString());
        firstOutboundUser2.setTitle("foo" + UUID.randomUUID().toString());
        barSource.output().send(MessageBuilder.withPayload(firstOutboundUser2).build());
        MessageCollector barSourceMessageCollector = barSourceContext.getBean(MessageCollector.class);
        Message<?> barOutboundMessage = barSourceMessageCollector.forChannel(barSource.output()).poll(1000,
                TimeUnit.MILLISECONDS);

        assertThat(barOutboundMessage).isNotNull();

        DocumentImpl secondUser2OutboundPojo = new DocumentImpl();
        secondUser2OutboundPojo.setId$1(UUID.randomUUID().toString());
        secondUser2OutboundPojo.setTitle("foo" + UUID.randomUUID().toString());
        source.output().send(MessageBuilder.withPayload(secondUser2OutboundPojo).build());
        Message<?> secondBarOutboundMessage = sourceMessageCollector.forChannel(source.output()).poll(1000,
                TimeUnit.MILLISECONDS);

        ConfigurableApplicationContext sinkContext = SpringApplication.run(AvroSinkApplication.class,
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.cloud.stream.schemaRegistryClient.enabled=false",
                "--schemaLocation=classpath:schemas/document_v1.schema");
        Sink sink = sinkContext.getBean(Sink.class);
        sink.input().send(outboundMessage);
        sink.input().send(barOutboundMessage);
        sink.input().send(secondBarOutboundMessage);
        List<DocumentImpl> receivedUsers = sinkContext.getBean(AvroSinkApplication.class).receivedUsers;
        assertThat(receivedUsers).hasSize(3);
        assertThat(receivedUsers.get(0)).isNotSameAs(firstOutboundFoo);
        assertThat(receivedUsers.get(0).getId$1()).isEqualTo(firstOutboundFoo.getId$1());
        assertThat(receivedUsers.get(0).getTitle()).isEqualTo(firstOutboundFoo.getTitle());

        assertThat(receivedUsers.get(1)).isNotSameAs(firstOutboundUser2);
        assertThat(receivedUsers.get(1).getId$1()).isEqualTo(firstOutboundUser2.getId$1());
        assertThat(receivedUsers.get(1).getTitle()).isEqualTo(firstOutboundUser2.getTitle());

        assertThat(receivedUsers.get(2)).isNotSameAs(secondUser2OutboundPojo);
        assertThat(receivedUsers.get(2).getId$1()).isEqualTo(secondUser2OutboundPojo.getId$1());
        assertThat(receivedUsers.get(2).getTitle()).isEqualTo(secondUser2OutboundPojo.getTitle());

        sourceContext.close();
    }

    @Test
    public void testSendMessageWithoutLocation() throws Exception {
        ConfigurableApplicationContext sourceContext = SpringApplication.run(AvroSourceApplication.class,
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.cloud.stream.schemaRegistryClient.enabled=false",
                "--spring.cloud.stream.bindings.output.contentType=avro/bytes");
        Source source = sourceContext.getBean(Source.class);
        DocumentImpl firstOutboundFoo = new DocumentImpl();
        firstOutboundFoo.setId$1(UUID.randomUUID().toString());
        firstOutboundFoo.setTitle("foo" + UUID.randomUUID().toString());
        source.output().send(MessageBuilder.withPayload(firstOutboundFoo).build());
        MessageCollector sourceMessageCollector = sourceContext.getBean(MessageCollector.class);
        Message<?> outboundMessage = sourceMessageCollector.forChannel(source.output()).poll(1000,
                TimeUnit.MILLISECONDS);

        ConfigurableApplicationContext barSourceContext = SpringApplication.run(AvroSourceApplication.class,
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.cloud.stream.schemaRegistryClient.enabled=false",
                "--spring.cloud.stream.bindings.output.contentType=avro/bytes");
        Source barSource = barSourceContext.getBean(Source.class);
        DocumentImpl firstOutboundUser2 = new DocumentImpl();
        firstOutboundUser2.setId$1(UUID.randomUUID().toString());
        firstOutboundUser2.setTitle("foo" + UUID.randomUUID().toString());
        barSource.output().send(MessageBuilder.withPayload(firstOutboundUser2).build());
        MessageCollector barSourceMessageCollector = barSourceContext.getBean(MessageCollector.class);
        Message<?> barOutboundMessage = barSourceMessageCollector.forChannel(barSource.output()).poll(1000,
                TimeUnit.MILLISECONDS);

        assertThat(barOutboundMessage).isNotNull();

        DocumentImpl secondUser2OutboundPojo = new DocumentImpl();
        secondUser2OutboundPojo.setId$1(UUID.randomUUID().toString());
        secondUser2OutboundPojo.setTitle("foo" + UUID.randomUUID().toString());
        source.output().send(MessageBuilder.withPayload(secondUser2OutboundPojo).build());
        Message<?> secondBarOutboundMessage = sourceMessageCollector.forChannel(source.output()).poll(1000,
                TimeUnit.MILLISECONDS);

        ConfigurableApplicationContext sinkContext = SpringApplication.run(AvroSinkApplication.class,
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.cloud.stream.schemaRegistryClient.enabled=false");
        Sink sink = sinkContext.getBean(Sink.class);
        sink.input().send(outboundMessage);
        sink.input().send(barOutboundMessage);
        sink.input().send(secondBarOutboundMessage);
        List<DocumentImpl> receivedUsers = sinkContext.getBean(AvroSinkApplication.class).receivedUsers;
        assertThat(receivedUsers).hasSize(3);
        assertThat(receivedUsers.get(0)).isNotSameAs(firstOutboundFoo);
        assertThat(receivedUsers.get(0).getId$1()).isEqualTo(firstOutboundFoo.getId$1());
        assertThat(receivedUsers.get(0).getTitle()).isEqualTo(firstOutboundFoo.getTitle());

        assertThat(receivedUsers.get(1)).isNotSameAs(firstOutboundUser2);
        assertThat(receivedUsers.get(1).getId$1()).isEqualTo(firstOutboundUser2.getId$1());
        assertThat(receivedUsers.get(1).getTitle()).isEqualTo(firstOutboundUser2.getTitle());

        assertThat(receivedUsers.get(2)).isNotSameAs(secondUser2OutboundPojo);
        assertThat(receivedUsers.get(2).getId$1()).isEqualTo(secondUser2OutboundPojo.getId$1());
        assertThat(receivedUsers.get(2).getTitle()).isEqualTo(secondUser2OutboundPojo.getTitle());

        sourceContext.close();
    }

    @EnableBinding(Source.class)
    @EnableAutoConfiguration
    @ConfigurationProperties
    public static class AvroSourceApplication {

        private Resource schemaLocation;

//        @Bean
//        public SchemaRegistryClient schemaRegistryClient() {
//            return stubSchemaRegistryClient;
//        }

        public void setSchemaLocation(Resource schemaLocation) {
            this.schemaLocation = schemaLocation;
        }

        @Bean
        @StreamMessageConverter
        public MessageConverter userMessageConverter() throws IOException {
            AvroSchemaMessageConverter avroSchemaMessageConverter = new AvroSchemaMessageConverter(
                    MimeType.valueOf("avro/bytes"));
            if (schemaLocation != null) {
                avroSchemaMessageConverter.setSchemaLocation(schemaLocation);
            }
            return avroSchemaMessageConverter;
        }
    }

    @EnableBinding(Sink.class)
    @EnableAutoConfiguration
    @ConfigurationProperties
    public static class AvroSinkApplication {

        public List<DocumentImpl> receivedUsers = new ArrayList<>();

        private Resource schemaLocation;

        @StreamListener(Sink.INPUT)
        public void listen(DocumentImpl user) {
            receivedUsers.add(user);
        }

        public void setSchemaLocation(Resource schemaLocation) {
            this.schemaLocation = schemaLocation;
        }

        @Bean
        @StreamMessageConverter
        public MessageConverter userMessageConverter() throws IOException {
            AvroSchemaMessageConverter avroSchemaMessageConverter = new AvroSchemaMessageConverter(
                    MimeType.valueOf("avro/bytes"));
            if (schemaLocation != null) {
                avroSchemaMessageConverter.setSchemaLocation(schemaLocation);
            }
            return avroSchemaMessageConverter;
        }

    }

}

