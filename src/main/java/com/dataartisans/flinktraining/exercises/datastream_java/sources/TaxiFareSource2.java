package com.dataartisans.flinktraining.exercises.datastream_java.sources;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataDiagnosis;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataTaxiFare;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;

public class TaxiFareSource2 implements SourceFunction<DataTaxiFare> {

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream FStream;

    public TaxiFareSource2(String dataFilePath) {
        this(dataFilePath, 1);
    }

    public TaxiFareSource2(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<DataTaxiFare> sourceContext) throws Exception {
        FStream = new FileInputStream(dataFilePath);
        reader = new BufferedReader(new InputStreamReader(FStream, "UTF-8"));

        String line;
        long time;
        while (reader.ready() && (line = reader.readLine()) != null) {
            DataTaxiFare TaxiFare = DataTaxiFare.instanceFromString(line);
            if (TaxiFare == null){
                continue;
            }
            time = getEventTime(TaxiFare);
            sourceContext.collectWithTimestamp(TaxiFare, time);
            sourceContext.emitWatermark(new Watermark(time - 1));
        }

        this.reader.close();
        this.reader = null;
        this.FStream.close();
        this.FStream = null;
    }

    public long getEventTime(DataTaxiFare TaxiFare) {
        return TaxiFare.getEventTime();
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
