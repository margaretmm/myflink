package com.dataartisans.flinktraining.exercises.datastream_java.sources;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataDiagnosis;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataEnergy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;

public class EnergySource implements SourceFunction<DataEnergy> {

    private final String dataFilePath;
    private transient BufferedReader reader;
    private transient InputStream FStream;

    public EnergySource(String dataFilePath) {
        this(dataFilePath, 1);
    }

    public EnergySource(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
    }

    @Override
    public void run(SourceContext<DataEnergy> sourceContext) throws Exception {
        FStream = new FileInputStream(dataFilePath);
        reader = new BufferedReader(new InputStreamReader(FStream, "UTF-8"));

        String line;
        long time;
        while (reader.ready() && (line = reader.readLine()) != null) {
            DataEnergy enery = DataEnergy.instanceFromString(line);
            if (enery == null){
                continue;
            }
            time = getEventTime(enery);
            sourceContext.collectWithTimestamp(enery, time);
            sourceContext.emitWatermark(new Watermark(time - 1));
        }

        this.reader.close();
        this.reader = null;
        this.FStream.close();
        this.FStream = null;
    }

    public long getEventTime(DataEnergy diag) {
        return diag.getEventTime();
    }

    @Override
    public void cancel()
    {
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
