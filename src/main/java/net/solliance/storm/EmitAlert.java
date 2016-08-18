package net.solliance.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;


public class EmitAlert extends BaseFunction {

    protected double minAlertTemp;
    protected double maxAlertTemp;

    public EmitAlert(double minTemp, double maxTemp) {
        minAlertTemp = minTemp;
        maxAlertTemp = maxTemp;
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {

        double tempReading = tuple.getDouble(1);
        String createDate = tuple.getString(2);
        String deviceId = tuple.getString(3);

        if (tempReading > maxAlertTemp )
        {

            collector.emit(new Values (
                    "reading above bounds",
                    tempReading,
                    createDate,
                    deviceId
            ));
            System.out.println("Emitting above bounds: " + tempReading);
        } else if (tempReading < minAlertTemp)
        {
            collector.emit(new Values (
                    "reading below bounds",
                    tempReading,
                    createDate,
                    deviceId
            ));
            System.out.println("Emitting below bounds: " + tempReading);
        }

    }

}