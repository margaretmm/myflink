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
import com.dataartisans.flinktraining.exercises.datastream_java.sources.DiagSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class diagnosisDataWashForRF {


	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = "D:/code/dataML/diagnosis.csv";
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<DataDiagnosis> DsDiag = env.addSource(
				new DiagSource(input, servingSpeedFactor));

		DataStream<DataDiagnosis> modDataStr = DsDiag
				.filter(new NoneFilter());
		DataStream<String> dataStrForLR = modDataStr.flatMap(new diagFlatMapForRF());
		dataStrForLR.print();
		dataStrForLR.writeAsText("./diagDataForRF");

//		DataStream<String> modDataStrForKmeans = modDataStr.flatMap(new diagFlatMapForKmeans());
//		modDataStrForKmeans.print();
//		modDataStrForKmeans.writeAsText("./diagModDataForKmeans");

//		DataStream<String> diagFlatMapForKmeansMLib = modDataStr.flatMap(new diagFlatMapForKmeansMLib());
//		diagFlatMapForKmeansMLib.print();
//		diagFlatMapForKmeansMLib.writeAsText("./diagFlatMapForKmeansMLib");

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



	public static class NoneFilter implements FilterFunction<DataDiagnosis> {

		@Override
		public boolean filter(DataDiagnosis diag) throws Exception {
			return IsNotNone(diag.Burning_urethra) && IsNotNone(diag.decision_renal_pelvis) &&IsNotNone(diag.decision_urinary)
					 && IsNotNone(diag.Lumbar_pain)  && IsNotNone(diag.Micturition_pains) && IsNotNone(diag.nausea)
					&& IsNotNone(diag.Urine_pushing)&& IsNotNone(diag.Temp)&& IsNotNone(diag.what) ;
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
	public static class diagFlatMapForRF implements FlatMapFunction<DataDiagnosis, String> {

		@Override
		public void flatMap(DataDiagnosis InputDiag, Collector<String> collector) throws Exception {
			DataDiagnosis diag = InputDiag;
			StringBuilder sb = new StringBuilder();
			sb.append(diag.Temp).append(",");
			sb.append(diag.what).append(",");
			sb.append(quantize(diag.nausea)).append(",");
			sb.append(quantize(diag.Lumbar_pain)).append(",");
			sb.append(quantize(diag.Urine_pushing)).append(",");
			sb.append(quantize(diag.Micturition_pains)).append(",");
			sb.append(quantize(diag.Burning_urethra)).append(",");
			sb.append(newLabel(diag.decision_urinary,diag.decision_renal_pelvis));

			collector.collect(sb.toString());
		}

		private String quantize(String val){
			if ("yes".equals(val)){
				return "1";
			}else if("no".equals(val)){
				return "0";
			}else{
				throw new RuntimeException("Invalid val(must either be 'yes' or 'no'): " + val);
			}
		}

		private String newLabel(String decision_urinary,String decision_renal_pelvis){
			if ("yes".equals(decision_urinary) && "no".equals(decision_renal_pelvis)){
				return "0";
			}else if("yes".equals(decision_renal_pelvis) && "no".equals(decision_urinary)){
				return "1";
			}else if("no".equals(decision_renal_pelvis) && "no".equals(decision_urinary)){
				return "2";
			}else if("yes".equals(decision_renal_pelvis) && "yes".equals(decision_urinary)){
				return "4";
			}else{
				throw new RuntimeException("Invalid decision_urinary/decision_renal_pelvis(must either be 'yes' or 'no'): decision_urinary=" + decision_urinary+",decision_renal_pelvis="+decision_renal_pelvis);
			}
		}
	}




}
