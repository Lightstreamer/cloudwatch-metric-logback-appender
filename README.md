# Lightstreamer logback appender for AWS CloudWatch Metric

## CloudWatch Metric Logback Appender

Send asynchronously Lightstreamer server statistics to Amazon CloudWatch Metric.

### Requirements:
 - Amazon IAM user with 'CloudWatchFullAccess' (arn:aws:iam::aws:policy/CloudWatchFullAccess) policy
 
 or
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:putMetricData"
    ],
      "Resource": [
        "*"
    ]
  }
 ]
}
```

### Usage:

Setup `lightstreamer_log_conf.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>

	<appender name="AWS_LS_METRIC" class="com.lightstreamer.cloudwatch_metric.logback.appender.AwsMetricAppender">
		<namespace>Lightstreamer</namespace>
	</appender>

    <logger name="LightstreamerMonitorTAB" level="info">
        <appender-ref ref="AWS_LS_METRIC"/>
    </logger>

</configuration>
```

#### Properties:

| Property       | Required  | Description                                           |
| :------------- | :-------: | :---------------------------------------------------- |
| namespace      | no        | CloudWatch metric name.                               |
| dimensions     | no        | Extra dimension in `java.util.Properties` text format |
