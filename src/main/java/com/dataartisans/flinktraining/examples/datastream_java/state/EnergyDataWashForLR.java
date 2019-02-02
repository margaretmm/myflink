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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataDiagnosis;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataEnergy;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.DiagSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.EnergySource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TempPredictionModelT1ToT3;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.Null;
import org.apache.flink.util.Collector;

public class EnergyDataWashForLR {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = "D:/code/dataML/energydata_complete1.csv";
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<DataEnergy> DsDiag = env.addSource(
				new EnergySource(input, servingSpeedFactor));

		DataStream<String> modDataStrForLR = DsDiag
				.filter(new NoneFilter())
				.map(new mapTime()).keyBy(0)
				.flatMap(new T3PredictionModel())//使用T1 预测T3 因为相关性大
				.flatMap(new energyFlatMapForLR());
		//modDataStr2.print();
		modDataStrForLR.writeAsText("./energyDataForLR");

		// run the prediction pipeline
		env.execute("EnergyData Prediction");
	}


	public static class mapTime implements MapFunction<DataEnergy, Tuple2<Long, DataEnergy>> {
		@Override
		public Tuple2<Long, DataEnergy> map(DataEnergy energy) throws Exception {
			long time = energy.eventTime.getMillis();;

			return new Tuple2<>(time, energy);
		}
	}

	public static class NoneFilter implements FilterFunction<DataEnergy> {

		@Override
		public boolean filter(DataEnergy energy) throws Exception {
			return IsNotNone(energy.Appliances) && IsNotNone(energy.date) &&IsNotNone(energy.Press_mm_hg)
					 && IsNotNone(energy.RH_1)  && IsNotNone(energy.RH_2)  ;
		}

		public boolean IsNotNone(Object Data){
			if (Data == null )
				return false;
			else
				return true;
		}
	}

    //使用Spark ML RF, 数据格式保存为csv常见格式
	// label 转换成一个二元变量表示
	public static class energyFlatMapForLR implements FlatMapFunction<DataEnergy, String> {

		@Override
		public void flatMap(DataEnergy InputDiag, Collector<String> collector) throws Exception {
			DataEnergy diag = InputDiag;
			StringBuilder sb = new StringBuilder();
			//sb.append(diag.date).append(",");
			sb.append(diag.lights).append(",");
			sb.append(diag.T1).append(",");
			sb.append(diag.RH_1).append(",");
			sb.append(diag.T2).append(",");
			sb.append(diag.RH_2).append(",");
			sb.append(diag.T3).append(",");
			sb.append(diag.RH_3).append(",");
			sb.append(diag.T4).append(",");
			sb.append(diag.RH_4).append(",");
			sb.append(diag.T5).append(",");
			sb.append(diag.RH_5).append(",");
			sb.append(diag.T6).append(",");
			sb.append(diag.RH_6).append(",");
			sb.append(diag.Press_mm_hg).append(",");
			sb.append(diag.RH_out).append(",");
			sb.append(diag.Windspeed).append(",");
			sb.append(diag.Visibility).append(",");
			sb.append(diag.Tdewpoint);//.append(",");

			collector.collect(sb.toString());
		}
	}

	public static class T3PredictionModel extends RichFlatMapFunction<Tuple2<Long, DataEnergy> , DataEnergy> {

		private transient ValueState<TempPredictionModelT1ToT3> modelState;

		@Override
		public void flatMap(Tuple2<Long, DataEnergy>  val, Collector<DataEnergy> out) throws Exception {

			// fetch operator state
			TempPredictionModelT1ToT3 model = modelState.value();
			if (model == null) {
				model = new TempPredictionModelT1ToT3();
			}

			DataEnergy energy = val.f1;
			// compute distance and direction

			if (energy.T3 == null) {
				energy.T3 = model.predictMissTmep(energy.T1);
				// emit prediction
				out.collect(energy);
			} else {
				// we have an end event: Update model
				// refine model
				model.refineModel(energy.T1, energy.T3);
				modelState.update(model);
				out.collect(energy);
			}
		}

		@Override
		public void open(Configuration config) {
			// obtain key-value state for prediction model
			ValueStateDescriptor<TempPredictionModelT1ToT3> descriptor =
					new ValueStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(TempPredictionModelT1ToT3.class));
			modelState = getRuntimeContext().getState(descriptor);
		}
	}
}
