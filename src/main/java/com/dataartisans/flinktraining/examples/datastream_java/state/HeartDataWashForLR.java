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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataEnergy;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataHeart;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataHeartWashed;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataTaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.EnergySource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.HeartSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TempPredictionModelT1ToT3;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.Math.abs;

public class HeartDataWashForLR {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = "D:/code/dataML/heartNoHead.csv";
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<DataHeart> DsHeart = env.addSource(
				new HeartSource(input, servingSpeedFactor));

		DataStream<String> modDataStrForLR = DsHeart
				.filter(new NoneFilter())
				.map(new mapTime()).keyBy(0)
				.flatMap(new NullFillMean())//使用T1 预测T3 因为相关性大
				.flatMap(new heartFlatMapForLR());
		//modDataStr2.print();
		modDataStrForLR.writeAsText("./HeartDataForLR");
		// run the prediction pipeline
		env.execute("HeartData Prediction");
	}


	public static class mapTime implements MapFunction<DataHeart, Tuple2<Long, DataHeart>> {
		@Override
		public Tuple2<Long, DataHeart> map(DataHeart energy) throws Exception {
			long time = energy.eventTime.getMillis();;

			return new Tuple2<>(time, energy);
		}
	}

	public static class NoneFilter implements FilterFunction<DataHeart> {

		@Override
		public boolean filter(DataHeart heart) throws Exception {
			return IsNotNone(heart.age) && IsNotNone(heart.angina) &&IsNotNone(heart.bloodPressure)
					 && IsNotNone(heart.bloodSugar)  && IsNotNone(heart.chestPain)  ;
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
	public static class heartFlatMapForLR implements FlatMapFunction<DataHeart, String> {

		@Override
		public void flatMap(DataHeart InputDiag, Collector<String> collector) throws Exception {
			DataHeart heart = InputDiag;
			DataHeartWashed heartWashed= new DataHeartWashed();
			StringBuilder sb = new StringBuilder();
			//sb.append(diag.date).append(",");
			if (heart.age<20){
				heartWashed.ageTeenage=1;
			}else if(heart.age<30){
				heartWashed.ageYoung=1;
			}else if(heart.age<50){
				heartWashed.ageMid=1;
			}else {
				heartWashed.ageOld=1;
			}

			if (heart.chestPain==0){
				heartWashed.chestPain0=1;
			}else if(heart.chestPain==1){
				heartWashed.chestPain1=1;
			}else if(heart.chestPain==2){
				heartWashed.chestPain2=1;
			}else {
				heartWashed.chestPain3=1;
			}

			if (heart.electrocardiographic==0){
				heartWashed.electrocardiographic0=1;
			}else if(heart.electrocardiographic==1){
				heartWashed.electrocardiographic1=1;
			}else{
				heartWashed.electrocardiographic2=1;
			}

			if (heart.peakExerciseSTSegment==0){
				heartWashed.slope0=1;
			}else if(heart.peakExerciseSTSegment==1){
				heartWashed.slope1=1;
			}else{
				heartWashed.slope2=1;
			}

			if (heart.thal==0){
				heartWashed.thal0=1;
			}else if(heart.thal==1){
				heartWashed.thal1=1;
			}else if(heart.thal==1){
				heartWashed.thal2=1;
			}else{
				heartWashed.thal3=1;
			}

			sb.append(heartWashed.ageTeenage).append(",");
			sb.append(heartWashed.ageYoung).append(",");
			sb.append(heartWashed.ageMid).append(",");
			sb.append(heartWashed.ageOld).append(",");
			sb.append(heart.sex).append(",");
			sb.append(heartWashed.chestPain0).append(",");
			sb.append(heartWashed.chestPain1).append(",");
			sb.append(heartWashed.chestPain2).append(",");
			sb.append(heartWashed.chestPain3).append(",");
			sb.append(heart.bloodPressure).append(",");
			sb.append(heart.serumCholestoral).append(",");
			sb.append(heart.bloodSugar).append(",");
			sb.append(heartWashed.electrocardiographic0).append(",");
			sb.append(heartWashed.electrocardiographic1).append(",");
			sb.append(heartWashed.electrocardiographic2).append(",");
			sb.append(heart.maximumHeartRate).append(",");
			sb.append(heart.angina).append(",");
			sb.append(heart.oldpeak).append(",");
			sb.append(heartWashed.slope0).append(",");
			sb.append(heartWashed.slope1).append(",");
			sb.append(heartWashed.slope2).append(",");
			sb.append(heart.numberOfMajorVessels).append(",");
			sb.append(heartWashed.thal0).append(",");
			sb.append(heartWashed.thal1).append(",");
			sb.append(heartWashed.thal2).append(",");
			sb.append(heartWashed.thal3).append(",");
			sb.append(heart.target);

			collector.collect(sb.toString());
		}
	}

	public static class NullFillMean extends RichFlatMapFunction<Tuple2<Long, DataHeart> ,DataHeart> {
		//private transient ValueState<Double> heartMeanState;
		private transient ListState<Integer> heartMeanState;
		private List<Integer> meansList;

		@Override
		public void flatMap(Tuple2<Long, DataHeart>  val, Collector< DataHeart> out) throws Exception {
			Iterator<Integer> modStateLst = heartMeanState.get().iterator();
			Integer MeanBloodPressure=null;
			Integer MeanBloodSugar=null;

			if(!modStateLst.hasNext()){
				MeanBloodPressure = 128;
				MeanBloodSugar = 1;
			}else{
				MeanBloodPressure=modStateLst.next();
				MeanBloodSugar=modStateLst.next();
			}

			meansList= new ArrayList<Integer>();
			meansList.add(MeanBloodPressure);
			meansList.add(MeanBloodSugar);

			DataHeart heart = val.f1;

			if(heart.bloodPressure == null){
				heart.bloodPressure= MeanBloodPressure;
				out.collect(heart);
			}else if(heart.bloodSugar == null){
				heart.bloodSugar= MeanBloodSugar;
				out.collect(heart);
			}else
			{
				if (abs(MeanBloodPressure-heart.bloodPressure)<50 && abs(MeanBloodSugar-heart.bloodSugar)<8) {
					heartMeanState.update(meansList);
					out.collect(heart);
				}
			}
		}

		@Override
		public void open(Configuration config) {
			ListStateDescriptor<Integer> descriptor2 =
					new ListStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(Integer.class));
			heartMeanState = getRuntimeContext().getListState(descriptor2);
		}
	}
}
