package com.lightstreamer.cloudwatch_metric.logback.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.*;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.*;

/**
 * Send asynchronously Lightstreamer server statistics to Amazon CloudWatch Metric.
 */
public final class AwsMetricAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private String namespace = "Lightstreamer";

    private final AmazonCloudWatchAsync cw =
            AmazonCloudWatchAsyncClientBuilder.defaultClient();
    private static final Map.Entry[] FIELD_DESCRIPTORS = new Map.Entry[]{
            new AbstractMap.SimpleImmutableEntry<>("Threads", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("HeapTotal", StandardUnit.Bytes),
            new AbstractMap.SimpleImmutableEntry<>("HeapFree", StandardUnit.Bytes),
            new AbstractMap.SimpleImmutableEntry<>("Sessions", StandardUnit.Count),
            null, // max sessions
            new AbstractMap.SimpleImmutableEntry<>("SessionsAdded", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("SessionsClosed", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("Connections", StandardUnit.Count),
            null, // max connections
            new AbstractMap.SimpleImmutableEntry<>("ConnectionsAdded", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("ConnectionsClosed", StandardUnit.Count),
            null, // separator
            new AbstractMap.SimpleImmutableEntry<>("ThreadsInPool", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("ThreadsActive", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("ThreadsAvailable", StandardUnit.None),
            new AbstractMap.SimpleImmutableEntry<>("TasksQueued", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("PoolQueueWait", StandardUnit.Milliseconds),
            new AbstractMap.SimpleImmutableEntry<>("NioWriteQueue", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("NioWriteQueueWait", StandardUnit.Milliseconds),
            new AbstractMap.SimpleImmutableEntry<>("NioWriteSelectors", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("NioTotalSelectors", StandardUnit.Count),
            null, // separator
            new AbstractMap.SimpleImmutableEntry<>("ItemsSubscribed", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("ItemsSubscribedClient", StandardUnit.Count),
            new AbstractMap.SimpleImmutableEntry<>("InboundThroughput", StandardUnit.CountSecond),
            new AbstractMap.SimpleImmutableEntry<>("InboundPrefiltered", StandardUnit.CountSecond),
            new AbstractMap.SimpleImmutableEntry<>("InboundThroughput", StandardUnit.CountSecond),
            new AbstractMap.SimpleImmutableEntry<>("InboundThroughputNet", StandardUnit.KilobitsSecond),
            null, // max outbound throughput (kbit/s)
            new AbstractMap.SimpleImmutableEntry<>("LostUpdate", StandardUnit.Count),
            null, // total lost updates
            null, // total bytes sent
            null, // separator
            new AbstractMap.SimpleImmutableEntry<>("ClientMessagesInboundThroughput", StandardUnit.CountSecond),
            new AbstractMap.SimpleImmutableEntry<>("ClientMessagesInboundThroughputNet", StandardUnit.KilobitsSecond),
            null, // max client messages throughput (kbit/s)
            null, // total messages handled
            new AbstractMap.SimpleImmutableEntry<>("ExtraSleepTime", StandardUnit.Milliseconds),
            new AbstractMap.SimpleImmutableEntry<>("ExtraNotifyTime", StandardUnit.Milliseconds),
            null // time
    };

    private final List<Dimension> dimensions = new ArrayList<>();

    public AwsMetricAppender() {
        try {
            final Dimension dimension = new Dimension()
                    .withName("hostname")
                    .withValue(InetAddress.getLocalHost().getHostName());
            dimensions.add(dimension);
        } catch (final Exception e) {
        }
    }

    @Override
    protected void append(final ILoggingEvent eventObject) {
        try {
            final String[] fields = eventObject.getFormattedMessage().split(",");

            if (fields.length != FIELD_DESCRIPTORS.length)
                throw new IllegalArgumentException("Unable to parse event");

            if (fields[fields.length - 1].equals("time"))
                return; // skip header

            final List<MetricDatum> datumList = new ArrayList<>(FIELD_DESCRIPTORS.length);
            for (int i = 0; i < FIELD_DESCRIPTORS.length; i++) {
                final Map.Entry<String, StandardUnit> descriptor = FIELD_DESCRIPTORS[i];
                if (descriptor == null) continue;

                final MetricDatum datum = new MetricDatum()
                        .withMetricName(descriptor.getKey())
                        .withUnit(descriptor.getValue())
                        .withValue(Double.parseDouble(fields[i]))
                        .withDimensions(dimensions)
                        .withTimestamp(new Date(Long.parseLong(fields[fields.length - 1])));
                datumList.add(datum);
            }

            // max 20 metric data
            for (int i = 0; i < datumList.size(); i += 20) {
                final PutMetricDataRequest request = new PutMetricDataRequest()
                        .withNamespace(namespace)
                        .withMetricData(datumList.subList(i, Math.min(i + 20, datumList.size())));

                cw.putMetricDataAsync(request, new PutMetricHandler(eventObject));
            }
        } catch (final Exception e) {
            addError("Error while sending metric " + eventObject.getFormattedMessage(), e);
        }
    }

    public String getDimensions() {
        final Properties properties = new Properties();
        for (final Dimension dimension : dimensions) {
            properties.put(dimension.getName(), dimension.getValue());
        }
        final StringWriter writer = new StringWriter();
        try {
            properties.store(writer, null);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return writer.toString();
    }

    public void setDimensions(final String string) {
        dimensions.clear();
        try {
            final Properties properties = new Properties();
            properties.load(new StringReader(string));
            for (final Map.Entry entry : properties.entrySet()) {
                final Dimension dimension = new Dimension()
                        .withName(entry.getKey().toString())
                        .withValue(String.valueOf(entry.getValue()));
                dimensions.add(dimension);
            }
        } catch (final Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    private final class PutMetricHandler implements AsyncHandler<PutMetricDataRequest, PutMetricDataResult> {
        private final ILoggingEvent eventObject;

        private PutMetricHandler(final ILoggingEvent eventObject) {
            this.eventObject = eventObject;
        }

        @Override
        public void onError(final Exception e) {
            addWarn("Unable to put metric " + eventObject.getFormattedMessage(), e);
        }

        @Override
        public void onSuccess(PutMetricDataRequest request, PutMetricDataResult putMetricDataResult) {
        }
    }
}
