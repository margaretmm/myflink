package com.dataartisans.flinktraining.exercises.datastream_java.sources;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataDiagnosis;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;

public class DiagSource implements SourceFunction<DataDiagnosis> {

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream FStream;

    public DiagSource(String dataFilePath) {
        this(dataFilePath, 1);
    }

    public DiagSource(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<DataDiagnosis> sourceContext) throws Exception {
        FStream = new FileInputStream(dataFilePath);
        reader = new BufferedReader(new InputStreamReader(FStream, "UTF-8"));

        String line;
        long time;
        while (reader.ready() && (line = reader.readLine()) != null) {
            DataDiagnosis diag = DataDiagnosis.instanceFromString(line);
            if (diag == null){
                continue;
            }
            time = getEventTime(diag);
            sourceContext.collectWithTimestamp(diag, time);
            sourceContext.emitWatermark(new Watermark(time - 1));
        }

        this.reader.close();
        this.reader = null;
        this.FStream.close();
        this.FStream = null;
    }

    public long getEventTime(DataDiagnosis diag) {
        return diag.getEventTime();
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
