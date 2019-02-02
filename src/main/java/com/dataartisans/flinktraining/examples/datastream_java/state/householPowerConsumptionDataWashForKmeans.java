/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.examples.datastream_java.state;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.myqPower;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.myqPowerWashed;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.io.*;

/**
 * Java reference implementation for the "Travel Time Prediction" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to continuously train a regression model that predicts
 * the travel time of a taxi based on the information of taxi ride end events.
 * For taxi ride start events, the model should be queried to estimate its travel time.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class householPowerConsumptionDataWashForKmeans {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = "D:/code/dataML/household_power_consumption.txt";
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//env.enableCheckpointing(5000);

		// start the data generator
		DataStream<myqPower> power = env.addSource(
				new PowerSource(input, servingSpeedFactor));

		DataStream<myqPower> modDataStr = power
				.filter(new NoneFilter());
		DataStream<Tuple2<Integer,String>> modDataStrForLR = modDataStr.flatMap(new powerFlatMap())
				.keyBy(0);
		//modDataStr.print();
		//modDataStr.writeAsCsv("./PowerModData");

//		DataStream<String> modDataStrForKmeans = modDataStr.flatMap(new powerFlatMapForKmeans());
//		modDataStrForKmeans.print();
//		modDataStrForKmeans.writeAsText("./PowerModDataForKmeans");

		DataStream<String> powerFlatMapForKmeansMLib = modDataStr.flatMap(new powerFlatMapForKmeansMLib());
		powerFlatMapForKmeansMLib.print();
		powerFlatMapForKmeansMLib.writeAsText("./powerFlatMapForKmeansMLib");

		// print the predictions
//		DataStream<Tuple3<String,String,String>> ModData=modDataStr.map(new ModelDataTuple3());
//		ModData.writeAsCsv("./travalModDataT3");
//		DataStream<Tuple2<String,String>> ModDataTrain=modDataStr.map(new ModelDataTuple2Label());
//		//ModDataTrain.print();
//		ModDataTrain.writeAsCsv("./travalModDataTest");
//
//		DataStream<Tuple2<String,String>> ModDataTest=modDataStr.map(new ModelDataTuple2Vetor());
//		ModDataTest.print();
//		ModDataTest.writeAsCsv("./travalModDataTrain");
		// run the prediction pipeline
		env.execute("Taxi Ride Prediction");
	}

	public static class PowerSource implements SourceFunction<myqPower> {

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

	public static class NoneFilter implements FilterFunction<myqPower> {

		@Override
		public boolean filter(myqPower power) throws Exception {
			return DateIsNone(power.Date) && TimeIsNone(power.Time) && power.Global_active_power!='?'&& power.Voltage!='?'
					&& power.Global_intensity!='?'&& power.Sub_metering_1!='?'&& power.Sub_metering_2!='?'&& power.Sub_metering_3!='?' ;
		}

		public boolean DateIsNone(String Date){
			String[] arr = Date.split("/");
			if (arr.length==3)
				return true;
			else
				return false;
		}

		public boolean TimeIsNone(String Time){
			String[] arr = Time.split(":");
			if (arr.length==3)
				return true;
			else
				return false;
		}

	}

	/**
	 * Maps the taxi ride event to the grid cell of the destination location.
	 */
	public static class powerFlatMap implements FlatMapFunction<myqPower, Tuple2<Integer,String>> {

		@Override
		public void flatMap(myqPower myqPower, Collector<Tuple2<Integer,String>> collector) throws Exception {
			myqPower power = myqPower;
			myqPowerWashed powerWashed=new myqPowerWashed();

			int month =  Integer.parseInt(myqPower.Date.split("/")[1]);
			if(month >12){
				throw new RuntimeException("!!!!!!!!!!!!!!!!!!!!!!get month >12:"+ month);
			}
			else if(month >=11 || month<=2){
				powerWashed.winter=1;
			}else if(month>=2 && month<=4){
				powerWashed.Spring =1;
			}else if(month>4 && month <8){
				powerWashed.summer=1;
			}else{
				powerWashed.autumn=1;
			}

			int hour =  Integer.parseInt(myqPower.Time.split(":")[0]);
			if (hour >=7 && hour<12){
				powerWashed.morning=1;
			}else if(hour>=12 && hour<=14){
				powerWashed.noon =1;
			}else if(hour>14 && hour <18){
				powerWashed.afternoon=1;
			}else if(hour>=18 && hour <=23){
				powerWashed.evening=1;
			}else{
				powerWashed.beforeDawn=1;
			}
			powerWashed.Global_active_power=myqPower.Global_active_power;
			powerWashed.Global_reactive_power=myqPower.Global_reactive_power;
			powerWashed.Sub_metering_1=myqPower.Sub_metering_1;
			powerWashed.Sub_metering_2=myqPower.Sub_metering_2;
			powerWashed.Sub_metering_3=myqPower.Sub_metering_3;
			collector.collect(new Tuple2<>(month,powerWashed.toString()));
		}
	}

	public static class powerFlatMapForKmeans implements FlatMapFunction<myqPower, String> {

		@Override
		public void flatMap(myqPower myqPower, Collector<String> collector) throws Exception {
			myqPower power = myqPower;
			myqPowerWashed powerWashed=new myqPowerWashed();
			String month =  myqPower.Date.split("/")[1];

			StringBuilder sb = new StringBuilder();
			sb.append(month).append("  ");
			sb.append("1:").append(myqPower.Global_active_power).append(" ");
			sb.append("2:").append(myqPower.Global_reactive_power).append(" ");
			sb.append("3:").append(myqPower.Voltage).append(" ");
			sb.append("4:").append(myqPower.Sub_metering_1).append(" ");
			sb.append("5:").append(myqPower.Sub_metering_2).append(" ");
			sb.append("6:").append(myqPower.Sub_metering_3);

			collector.collect(sb.toString());
		}
	}

	public static class powerFlatMapForKmeansMLib implements FlatMapFunction<myqPower, String> {

		@Override
		public void flatMap(myqPower myqPower, Collector<String> collector) throws Exception {
			myqPower power = myqPower;
			String month =  myqPower.Date.split("/")[1];
			String hour =  myqPower.Time.split(":")[0];
			StringBuilder sb = new StringBuilder();
			sb.append(month).append(",");
			sb.append(myqPower.Global_active_power).append(",");
			sb.append(myqPower.Global_reactive_power).append(",");
//			sb.append(myqPower.Voltage).append(",");
//			sb.append(myqPower.Sub_metering_1).append(",");
//			sb.append(myqPower.Sub_metering_2).append(",");
			sb.append(myqPower.Sub_metering_3);

			collector.collect(sb.toString());
		}
	}

	public static class ModelDataTuple3 implements MapFunction<String,Tuple3<String, String,String>> {

		@Override
		public Tuple3<String, String, String> map(String val) throws Exception {
			// compute distance and direction
			String[] lst = val.split(" ");
			String direction = lst[0];
			String distance = lst[1];
			String travelTime = lst[2];

			return new Tuple3<>(direction, distance, travelTime);
		}
	}
}
