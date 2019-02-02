package com.dataartisans.flinktraining.exercises.datastream_java.sources;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.myqPower;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;

public class PowerSource implements SourceFunction<myqPower> {

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream FStream;


    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
     */
    public PowerSource(String dataFilePath) {
        this(dataFilePath, 1);
    }

    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served exactly in order of their time stamps
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the TaxiRide records are read.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public PowerSource(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<myqPower> sourceContext) throws Exception {
        FStream = new FileInputStream(dataFilePath);
        reader = new BufferedReader(new InputStreamReader(FStream, "UTF-8"));

        String line;
        long time;
        while (reader.ready() && (line = reader.readLine()) != null) {
            myqPower power = myqPower.instanceFromString(line);
            if (power == null){
                continue;
            }
            time = getEventTime(power);
            sourceContext.collectWithTimestamp(power, time);
            sourceContext.emitWatermark(new Watermark(time - 1));
        }

        this.reader.close();
        this.reader = null;
        this.FStream.close();
        this.FStream = null;
    }

    public long getEventTime(myqPower power) {
        return power.getEventTime();
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.FStream != null) {
                this.FStream.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.FStream = null;
        }
    }
}
