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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

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
public class TravelTimeDataWash {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		//final String input = params.getRequired("input");
		final String input = params.get("input", ExerciseBase.pathToRideData);
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// create a checkpoint every 5 seconds
		env.enableCheckpointing(5000);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new CheckpointedTaxiRideSource(input, servingSpeedFactor));

		DataStream<String> modDataStr = rides
			// filter out rides that do not start or stop in NYC
			.filter(new NYCFilter())
			// map taxi ride events to the grid cell of the destination
			.map(new GridCellMatcher())
			// organize stream by destination
			.keyBy(0)
			// predict and refine model per destination
			.map(new ModelDataStr());

		// print the predictions

		// write the filtered data to a Kafka sink
//		modDataStr.writeToSocket("127.0.0.1",9999, new SimpleStringSchema());

//		DataStream<Tuple3<String,String,String>> ModData=modDataStr.map(new ModelDataTuple3());
//		ModData.writeAsCsv("./travalModDataT3");
		DataStream<Tuple2<String,String>> ModDataTrain=modDataStr.map(new ModelDataTuple2Label());
		//ModDataTrain.print();
		ModDataTrain.writeAsCsv("./travalModDataTest");

		DataStream<Tuple2<String,String>> ModDataTest=modDataStr.map(new ModelDataTuple2Vetor());
		ModDataTest.print();
		ModDataTest.writeAsCsv("./travalModDataTrain");
		// run the prediction pipeline
		env.execute("Taxi Ride Prediction");
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

	/**
	 * Maps the taxi ride event to the grid cell of the destination location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, TaxiRide>> {

		@Override
		public Tuple2<Integer, TaxiRide> map(TaxiRide ride) throws Exception {
			int endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);

			return new Tuple2<>(endCell, ride);
		}
	}


	public static class ModelDataStr implements MapFunction<Tuple2<Integer, TaxiRide>,  String> {

		@Override
		public  String map (Tuple2<Integer, TaxiRide> val) throws Exception {

			TaxiRide ride = val.f1;
			// compute distance and direction
			double distance = GeoUtils.getEuclideanDistance(ride.startLon, ride.startLat, ride.endLon, ride.endLat);
			int direction = GeoUtils.getDirectionAngle(ride.endLon, ride.endLat, ride.startLon, ride.startLat);
			double travelTime = (ride.endTime.getMillis() - ride.startTime.getMillis()) / 60000.0;
			if (travelTime<0)
				travelTime =travelTime/10000000;

			return new String( String.valueOf(direction)+' '+ String .format("%.3f",distance)+' '+ String .format("%.3f",travelTime));
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

	public static class ModelDataTuple2Vetor implements MapFunction<String,Tuple2<String, String>> {
		@Override
		public Tuple2<String, String> map(String val) throws Exception {
			// compute distance and direction
			String[] lst = val.split(" ");
			String direction = lst[0];
			String distance = lst[1];

			return new Tuple2<>(direction, distance);
		}
	}

	public static class ModelDataTuple2Label implements MapFunction<String,Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(String val) throws Exception {
			// compute distance and direction
			String[] lst = val.split(" ");
			String direction = lst[0];
			String distance = lst[1];
			String travelTime = lst[2];

			return new Tuple2<>(travelTime,  distance+' '+direction);
		}
	}
}
