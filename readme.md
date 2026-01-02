-Dlog4j.configurationFile=classpath:log4j2.xml
-Dotel.datadog.enabled=true

-javaagent:/Users/ereshtayanna/Downloads/opentelemetry-javaagent.jar
-Dotel.javaagent.debug=true



### Install the Spark integration for Datadog Agent
    vi /opt/datadog-agent/etc/conf.d/spark.d/conf.yaml
    
    datadog-agent status
    sudo launchctl stop com.datadoghq.agent
    sudo launchctl start com.datadoghq.agent

### Test the integration configuration:
    sudo datadog-agent check spark

### restart the agent:
    launchctl kickstart -k gui/$(id -u)/com.datadoghq.agent

### Added below VM options 
    -Dotel.datadog.enabled=true 
    -Dlog4j.configurationFile=classpath:log4j2.xml --> Force app to use custom log4j2 config (so we can control appenders/formatting).
    -Dotel.service.name=otel-fraud-detection-app
    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.port=9010
    -Dcom.sun.management.jmxremote.rmi.port=9010
    -Dcom.sun.management.jmxremote.authenticate=false
    -Dcom.sun.management.jmxremote.ssl=false
    -Djava.rmi.server.hostname=127.0.0.1

    -- Final VM options for Spark Application (Driver / Executors):
    -Dotel.datadog.enabled=true
    -Dlog4j.configurationFile=classpath:log4j2.xml
    -Dspark.app.name=fraud-detection-app
    -Dotel.service.name=OTEL-SPARK-APP
    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.port=9010
    -Dcom.sun.management.jmxremote.rmi.port=9010
    -Dcom.sun.management.jmxremote.authenticate=false
    -Dcom.sun.management.jmxremote.ssl=false
    -Djava.rmi.server.hostname=127.0.0.1
    -javaagent:/Users/ereshtayanna/Downloads/opentelemetry-javaagent.jar
    -Dotel.traces.exporter=otlp
    -Dotel.exporter.otlp.endpoint=http://localhost:14317
    -Dotel.exporter.otlp.protocol=grpc

### Exposes JVM MBeans over JMX on 127.0.0.1:9010 so an external collector (Datadog Agent JMXFetch) can scrape:
    heap / non-heap
    GC metrics
    thread counts
    class loading, etc.


### Datadog Spark integration + JMX (port 9010).
    /opt/datadog-agent/etc/conf.d/spark.d/conf.yaml

### Send Spark metrics (driver/executor/JVM + partial Spark internals) from a local Spark standalone app (IntelliJ) to Datadog using JMX.
    ``
    
    Spark JVM (Driver / Executors)
                ↓  (JMX on port 9010)
    Datadog Agent (JMXFetch + Spark integration)
                ↓
    Datadog Dashboards
    
    ``
### Datadog Agent status , Ensure Agent & JMXFetch are running:
    sudo datadog-agent status | grep -i jmx
    sudo datadog-agent status | grep -i -E "jmx|jmxfetch|9010|collector"

## How to verify it’s actually working (critical)
### JVM must expose JMX, Run below command while Spark job is running:
    $ lsof -nP -iTCP:9010 -sTCP:LISTEN

### High-signal ones:

    FileScan / BatchScan → reading source (HDFS/S3/Hive table)
    Project → select/withColumn/rename (column projection)
    Filter → where/filter
    HashAggregate / ObjectHashAggregate → groupBy/agg
    Join (BroadcastHashJoin / SortMergeJoin / ShuffledHashJoin) → join type
    Exchange → shuffle boundary (usually the big cost driver)
    Sort → sort/orderBy
    GlobalLimit/LocalLimit → head/show/take-like actions
    WriteFiles / InsertIntoHadoopFsRelationCommand / SaveIntoDataSourceCommand → writes

    Read → Filter → Select → Join/Aggregate → Shuffle → Write


datadog queries:

    service:otel-spark-app AND status:error
    service:otel-spark-app AND resource_name:"Spark Driver Failure"
    service:otel-spark-app AND resource_name:"Exchange"



