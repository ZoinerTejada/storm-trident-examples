package net.solliance.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.storm.eventhubs.samples.TransactionalTridentEventCount;
import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.eventhubs.trident.OpaqueTridentEventHubSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;

import java.io.FileReader;
import java.util.Properties;


/**
 * To submit this topology:
 *
 * storm jar {jarfile} com.solliance.storm.AlertTopology {topologyname} config.properties
 *
 * make sure the config.properties file is also uploaded with the uber jar
 */
public class AlertTopology {

    protected EventHubSpoutConfig spoutConfig;
    protected int numWorkers;
    protected double minAlertTemp;
    protected double maxAlertTemp;

    public static void main(String[] args) throws Exception {
        AlertTopology scenario = new AlertTopology();

        String topologyName;
        String configPropertiesPath;
        if (args != null && args.length >0){
            topologyName = args[0];
            configPropertiesPath = args[1];
        }
        else
        {
            topologyName = "AlertTopology";
            configPropertiesPath = null;
        }


        scenario.loadAndApplyConfig(configPropertiesPath, topologyName);
        StormTopology topology = scenario.buildTopology();
        scenario.submitTopology(args, topology);
    }

    public AlertTopology(){

    }

    protected StormTopology buildTopology() {

        TridentTopology topology = new TridentTopology();

        OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);

        topology.newStream("stream-" + spoutConfig.getTopologyName(), spout)
                .each(new Fields("message"), new ParseTelemetry(), new Fields("temp", "createDate", "deviceId"))
                .each(new Fields("message", "temp", "createDate", "deviceId"), new EmitAlert(65, 68), new Fields("reason"))
                .parallelismHint(spoutConfig.getPartitionCount());

        return topology.build();
    }

    protected void submitTopology(String[] args, StormTopology topology) throws Exception {
        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], config, topology);
        } else {
            config.setMaxTaskParallelism(2);

            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("test", config, topology);
            Thread.sleep(600000);
            localCluster.shutdown();
        }
    }

    protected Properties loadConfigurationProperties(String configFilePath) throws Exception{
        Properties properties = new Properties();
        if(configFilePath != null) {
            properties.load(new FileReader(configFilePath));
        }
        else {
            properties.load(AlertTopology.class.getClassLoader().getResourceAsStream(
                    "config.properties"));
        }
        return properties;
    }

    protected void loadAndApplyConfig(String configFilePath, String topologyName) throws Exception {

        Properties properties = loadConfigurationProperties(configFilePath);

        String username = properties.getProperty("eventhubspout.username");
        String password = properties.getProperty("eventhubspout.password");
        String namespaceName = properties.getProperty("eventhubspout.namespace");
        String entityPath = properties.getProperty("eventhubspout.entitypath");
        String targetFqnAddress = properties.getProperty("eventhubspout.targetfqnaddress");
        String zkEndpointAddress = properties.getProperty("zookeeper.connectionstring");
        int partitionCount = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
        int checkpointIntervalInSeconds = Integer.parseInt(properties.getProperty("eventhubspout.checkpoint.interval"));
        int receiverCredits = Integer.parseInt(properties.getProperty("eventhub.receiver.credits"));
        String maxPendingMsgsPerPartitionStr = properties.getProperty("eventhubspout.max.pending.messages.per.partition");
        if(maxPendingMsgsPerPartitionStr == null) {
            maxPendingMsgsPerPartitionStr = "1024";
        }
        int maxPendingMsgsPerPartition = Integer.parseInt(maxPendingMsgsPerPartitionStr);
        String enqueueTimeDiffStr = properties.getProperty("eventhub.receiver.filter.timediff");
        if(enqueueTimeDiffStr == null) {
            enqueueTimeDiffStr = "0";
        }
        int enqueueTimeDiff = Integer.parseInt(enqueueTimeDiffStr);
        long enqueueTimeFilter = 0;
        if(enqueueTimeDiff != 0) {
            enqueueTimeFilter = System.currentTimeMillis() - enqueueTimeDiff*1000;
        }
        String consumerGroupName = properties.getProperty("eventhubspout.consumer.group.name");

        System.out.println("Eventhub spout config: ");
        System.out.println("  partition count: " + partitionCount);
        System.out.println("  checkpoint interval: " + checkpointIntervalInSeconds);
        System.out.println("  receiver credits: " + receiverCredits);

        spoutConfig = new EventHubSpoutConfig(username, password,
                namespaceName, entityPath, partitionCount, zkEndpointAddress,
                checkpointIntervalInSeconds, receiverCredits, maxPendingMsgsPerPartition,
                enqueueTimeFilter);

        if(targetFqnAddress != null)
        {
            spoutConfig.setTargetAddress(targetFqnAddress);
        }
        spoutConfig.setConsumerGroupName(consumerGroupName);

        //set the number of workers to be the same as partition number.
        //the idea is to have a spout and a partial count bolt co-exist in one
        //worker to avoid shuffling messages across workers in storm cluster.
        numWorkers = spoutConfig.getPartitionCount();

        spoutConfig.setTopologyName(topologyName);

        minAlertTemp = Double.parseDouble(properties.getProperty("alerts.mintemp"));
        maxAlertTemp = Double.parseDouble(properties.getProperty("alerts.maxtemp"));
    }

}