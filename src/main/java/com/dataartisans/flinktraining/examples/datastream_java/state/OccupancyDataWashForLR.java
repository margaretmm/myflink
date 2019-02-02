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


import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataOccupancy;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.occupancySource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.SimplePredictionModel;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class OccupancyDataWashForLR {

	public static void main(String[] args) throws Exception {

		final String input = "D:/code/dataML/occupancy_data/datatest_nohead.csv";
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<DataOccupancy> DsOccupancy = env.addSource(
				new occupancySource(input, servingSpeedFactor));

		DataStream<String> modDataStrForLR = DsOccupancy
				.map(new mapTime()).keyBy(0)
				.flatMap(new NullTempFillMean()).keyBy(0)
				.flatMap(new HumidityPridict()).keyBy(0)///使用HumidityRatio 预测THumidity因为相关性大
				.flatMap(new HumidityRatioPridict())//使用Humidity 预测THumidityRatio因为相关性大
				.flatMap(new OccupyFlatMapForLR());
		modDataStrForLR.print();
		modDataStrForLR.writeAsText("./OccupyDataForLR");

		// run the prediction pipeline
		env.execute("EnergyData Prediction");
	}


	public static class mapTime implements MapFunction<DataOccupancy, Tuple2<Long, DataOccupancy>> {
		@Override
		public Tuple2<Long, DataOccupancy> map(DataOccupancy Occupancy) throws Exception {
			long time = Occupancy.eventTime.getMillis();;

			return new Tuple2<>(time, Occupancy);
		}
	}

	//使用Spark ML RF, 数据格式保存为csv常见格式
	// label 转换成一个二元变量表示
	public static class OccupyFlatMapForLR implements FlatMapFunction<DataOccupancy, String> {

		@Override
		public void flatMap(DataOccupancy diag, Collector<String> collector) throws Exception {
			StringBuilder sb = new StringBuilder();
			sb.append(diag.Temp).append(",");
			sb.append(diag.Humidity).append(",");
			sb.append(diag.Light).append(",");
			sb.append(diag.CO2).append(",");
			sb.append(diag.HumidityRatio).append(",");
			sb.append(diag.Occupancy);

			collector.collect(sb.toString());
		}
	}

	public static class HumidityRatioPridict extends RichFlatMapFunction<Tuple2<Long, DataOccupancy> , DataOccupancy> {

		private transient ValueState<SimplePredictionModel> modelState;

		@Override
		public void flatMap(Tuple2<Long, DataOccupancy>  val, Collector<DataOccupancy> out) throws Exception {

			SimplePredictionModel HumidityRatioPridictModel = modelState.value();
			if (HumidityRatioPridictModel == null) {
				HumidityRatioPridictModel = new SimplePredictionModel();
			}
			DataOccupancy occupancy = val.f1;

			if(occupancy.Humidity != null && occupancy.HumidityRatio== null){
				occupancy.HumidityRatio = HumidityRatioPridictModel.predictY(occupancy.Humidity);
				out.collect(occupancy);
			}
			else {
				if(occupancy.Humidity != null && occupancy.HumidityRatio != null) {
					HumidityRatioPridictModel.refineModel(occupancy.Humidity, occupancy.HumidityRatio);
					modelState.update(HumidityRatioPridictModel);
					out.collect(occupancy);
				}else{
					//log Humidity,HumidityRatio are both null
					System.out.println("~~~~~!Humidity,HumidityRatio are both null");
				}
			}
		}

		@Override
		public void open(Configuration config) {
			// obtain key-value state for prediction model
			ValueStateDescriptor<SimplePredictionModel> descriptor =
					new ValueStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(SimplePredictionModel.class));
			modelState = getRuntimeContext().getState(descriptor);
		}
	}

	public static class HumidityPridict extends RichFlatMapFunction<Tuple2<Long, DataOccupancy> , Tuple2<Long, DataOccupancy>> {

		private transient ValueState<SimplePredictionModel> modelState;

		@Override
		public void flatMap(Tuple2<Long, DataOccupancy>  val, Collector<Tuple2<Long, DataOccupancy>> out) throws Exception {

			SimplePredictionModel HumidityRatioPridictModel = modelState.value();
			if (HumidityRatioPridictModel == null) {
				HumidityRatioPridictModel = new SimplePredictionModel();
			}
			DataOccupancy occupancy = val.f1;

			if(occupancy.Humidity != null && occupancy.HumidityRatio== null){
				occupancy.HumidityRatio = HumidityRatioPridictModel.predictY(occupancy.Humidity);
				out.collect(new Tuple2<>(val.f0,occupancy));
			}
			else {
				if(occupancy.Humidity != null && occupancy.HumidityRatio != null) {
					HumidityRatioPridictModel.refineModel(occupancy.Humidity, occupancy.HumidityRatio);
					modelState.update(HumidityRatioPridictModel);
					out.collect(new Tuple2<>(val.f0,occupancy));
				}else{
					//log Humidity,HumidityRatio are both null
					System.out.println("~~~~~!Humidity,HumidityRatio are both null");
				}
			}
		}

		@Override
		public void open(Configuration config) {
			// obtain key-value state for prediction model
			ValueStateDescriptor<SimplePredictionModel> descriptor =
					new ValueStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(SimplePredictionModel.class));
			modelState = getRuntimeContext().getState(descriptor);
		}
	}

	public static class NullTempFillMean extends RichFlatMapFunction<Tuple2<Long, DataOccupancy> , Tuple2<Long, DataOccupancy>> {

		private transient ValueState<Double> TemperatureMeanState;

		@Override
		public void flatMap(Tuple2<Long, DataOccupancy>  val, Collector<Tuple2<Long, DataOccupancy>> out) throws Exception {
			DataOccupancy occupancy = val.f1;
			// compute distance and direction
			Double TemperatureMean=TemperatureMeanState.value();
			if (TemperatureMean== null){
				TemperatureMean=0.0;
			}
			if(occupancy.Temp == null){
				occupancy.Temp= TemperatureMean;
			}else
			{
				TemperatureMean=(TemperatureMean+occupancy.Temp)/2;
				TemperatureMeanState.update(TemperatureMean);
			}
			out.collect(new Tuple2<>(val.f0,occupancy));
		}

		@Override
		public void open(Configuration config) {
				ValueStateDescriptor<Double> descriptor2 =
					new ValueStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(Double.class));
			TemperatureMeanState = getRuntimeContext().getState(descriptor2);
		}
	}
}
