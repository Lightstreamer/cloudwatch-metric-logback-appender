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
    private int storageResolution = 60;

    private AmazonCloudWatchAsync cw;

    private final List<Dimension> dimensions = new ArrayList<>();

    private int timeColumnPos = -1;
    private String[] columnTitles;
    private StandardUnit[] columnUnits;

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
    public void start() {
        super.start();
        cw = AmazonCloudWatchAsyncClientBuilder.defaultClient();
    }

    @Override
    protected void append(final ILoggingEvent eventObject) {
        try {
            final String[] fields = eventObject.getFormattedMessage().split(",");
            if (timeColumnPos <= 0) {
                parseHeader(fields);
                return;
            }

            if (fields.length != columnTitles.length)
                throw new IllegalArgumentException("Unable to parse event");

            if (fields[fields.length - 1].equals("time"))
                return; // skip header

            final List<MetricDatum> datumList = new ArrayList<>(columnTitles.length);
            final Date timestamp = new Date(Long.parseLong(fields[timeColumnPos]));
            for (int i = 0; i < columnTitles.length; i++) {
                final String metricName = columnTitles[i];
                if (metricName == null) continue;

                final MetricDatum datum = new MetricDatum()
                        .withMetricName(metricName)
                        .withUnit(columnUnits[i])
                        .withValue(Double.parseDouble(fields[i]))
                        .withDimensions(dimensions)
                        .withStorageResolution(storageResolution)
                        .withTimestamp(timestamp);
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
            if (getClass().desiredAssertionStatus())
                e.printStackTrace();
        }
    }

    private void parseHeader(final String[] headers) {
        columnTitles = new String[headers.length];
        columnUnits = new StandardUnit[headers.length];

        for (int i = 0; i < headers.length; i++) {
            final String header = headers[i];
            if (header.equals("separator")
                    || header.startsWith("max ")
                    || header.startsWith("total ") && !header.equals("total threads") && !header.equals("total heap"))
                continue;

            if (header.equals("time")) {
                timeColumnPos = i;
                continue;
            }

            final StandardUnit standardUnit;
            if (header.contains("time") || header.contains("wait")) {
                standardUnit = StandardUnit.Milliseconds;
            } else if (header.contains("heap")) {
                standardUnit = StandardUnit.Bytes;
            } else if (header.contains("kbit/s")) {
                standardUnit = StandardUnit.KilobitsSecond;
            } else if (header.contains("/s")) {
                standardUnit = StandardUnit.CountSecond;
            } else if (header.contains("added") || header.contains("closed")) {
                standardUnit = StandardUnit.Count;
            } else {
                standardUnit = StandardUnit.None;
            }

            columnTitles[i] = header
                    .replaceAll("\\W+", " ")
                    .trim()
                    .replace(' ', ' ');
            columnUnits[i] = standardUnit;
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

    public int getStorageResolution() {
        return storageResolution;
    }

    public void setStorageResolution(int storageResolution) {
        this.storageResolution = storageResolution;
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
