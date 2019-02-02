package com.dataartisans.flinktraining.exercises.datastream_java.sources;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataDiagnosis;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataOccupancy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;

public class occupancySource implements SourceFunction<DataOccupancy> {

    private final String dataFilePath;

    private transient BufferedReader reader;
    private transient InputStream FStream;

    public occupancySource(String dataFilePath) {
        this(dataFilePath, 1);
    }

    public occupancySource(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
    }

    @Override
    public void run(SourceContext<DataOccupancy> sourceContext) throws Exception {
        FStream = new FileInputStream(dataFilePath);
        reader = new BufferedReader(new InputStreamReader(FStream, "UTF-8"));

        String line;
        long time;
        while (reader.ready() && (line = reader.readLine()) != null) {
            DataOccupancy Occupancy = DataOccupancy.instanceFromString(line);
            if (Occupancy == null){
                continue;
            }
            time = getEventTime(Occupancy);
            sourceContext.collectWithTimestamp(Occupancy, time);
            sourceContext.emitWatermark(new Watermark(time - 1));
        }

        this.reader.close();
        this.reader = null;
        this.FStream.close();
        this.FStream = null;
    }

    public long getEventTime(DataOccupancy Occupancy) {
        return Occupancy.getEventTime();
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
