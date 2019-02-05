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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.DataTaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource2;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static java.lang.Math.abs;

public class TaxiFareDataWashForLR {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = "D:/code/dataML/taxi-fare.csv";
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<DataTaxiFare> DsTaxiFare = env.addSource(
				new TaxiFareSource2(input, servingSpeedFactor));

		SingleOutputStreamOperator<Tuple12<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Double, Integer, Integer, Integer, Double>> modDataStrForLR = DsTaxiFare
				.filter(new NoneFilter())
				.map(new mapTime()).keyBy(0)
				.flatMap(new NullFillMean())
				//.map(new map1())
				.map(new taxiFareFlatMapForLR());
		//modDataStr2.print();
		modDataStrForLR.writeAsCsv("./taxiFareDataForLR");

		// run the prediction pipeline
		env.execute("EnergyData Prediction");
	}

	public static class mapTime implements MapFunction<DataTaxiFare, Tuple2<Long, DataTaxiFare>> {
		@Override
		public Tuple2<Long, DataTaxiFare> map(DataTaxiFare TaxiFare) throws Exception {
			long time = TaxiFare.eventTime.getMillis();;

			return new Tuple2<>(time, TaxiFare);
		}
	}

	public static class taxiFareFlatMapForLR implements MapFunction<DataTaxiFare, Tuple12<Integer, Integer,Integer, Integer,Integer,Integer,Integer, Double,Integer,Integer,Integer,Double>> {
		@Override
		public Tuple12<Integer, Integer,Integer,Integer,Integer, Integer,Integer, Double,Integer,Integer,Integer,Double> map(DataTaxiFare TaxiFare) throws Exception {
			String venderId = TaxiFare.vendor_id;
			String paymentType = TaxiFare.payment_type;
			int vendorVTS=0;
			int vendorCMT=0;

			int paytypeUNK=0;
			int paytypeCRD=0;
			int paytypeDIS=0;
			int paytypeCSH=0;
			int paytypeNOC=0;

			if("VTS".equals(venderId)){
				vendorVTS=1;
			}else if("CMT".equals(venderId)){
				vendorCMT=1;
			}

			if("UNK".equals(paymentType)){
				paytypeUNK=1;
			}else if("CRD".equals(paymentType)){
				paytypeCRD=1;
			}else if("DIS".equals(paymentType)){
				paytypeDIS=1;
			}else if("CSH".equals(paymentType)){
				paytypeCSH=1;
			}else if("NOC".equals(paymentType)){
				paytypeNOC=1;
			}

			return new Tuple12<>(vendorVTS,vendorCMT,paytypeUNK,paytypeCRD,paytypeDIS,paytypeCSH,paytypeNOC,
					TaxiFare.trip_distance,TaxiFare.passenger_count,TaxiFare.trip_time_in_secs,TaxiFare.rate_code,TaxiFare.fare_amount);
		}
	}


	public static class NoneFilter implements FilterFunction<DataTaxiFare> {
		@Override
		public boolean filter(DataTaxiFare TaxiFare) throws Exception {
			return IsNotNone(TaxiFare.vendor_id) && IsNotNone(TaxiFare.payment_type) &&IsNotNone(TaxiFare.passenger_count)
					 && IsNotNone(TaxiFare.trip_distance)  && IsNotNone(TaxiFare.trip_time_in_secs)  ;
		}

		public boolean IsNotNone(Object Data){
			if (Data == null )
				return false;
			else
				return true;
		}
	}
//
//    //使用Spark ML RF, 数据格式保存为csv常见格式
//	// label 转换成一个二元变量表示
//	public static class taxiFareFlatMapForLR implements FlatMapFunction<DataTaxiFare, String> {
//		@Override
//		public void flatMap(DataTaxiFare InputDiag, Collector<String> collector) throws Exception {
//			DataTaxiFare TaxiFare = InputDiag;
//			StringBuilder sb = new StringBuilder();
//			//sb.append(diag.date).append(",");
//			sb.append(TaxiFare.vendor_id).append(",");
//			sb.append(TaxiFare.trip_time_in_secs).append(",");
//			sb.append(TaxiFare.passenger_count).append(",");
//			sb.append(TaxiFare.payment_type).append(",");
//			sb.append(TaxiFare.trip_distance).append(",");
//			sb.append(TaxiFare.rate_code).append(",");
//			sb.append(TaxiFare.fare_amount);
//			collector.collect(sb.toString());
//		}
//	}

	public static class NullFillMean extends RichFlatMapFunction<Tuple2<Long, DataTaxiFare> ,DataTaxiFare> {
		private transient ValueState<Double> FarePerDistanceMeanState;

		@Override
		public void flatMap(Tuple2<Long, DataTaxiFare>  val, Collector< DataTaxiFare> out) throws Exception {
			DataTaxiFare TaxiFare = val.f1;
			// compute distance and direction
			Double FarePerDistanceMean=FarePerDistanceMeanState.value();
			if (FarePerDistanceMean== null){
				FarePerDistanceMean=0.0;
			}
			if(TaxiFare.fare_amount == null && TaxiFare.trip_distance!= null){
				TaxiFare.fare_amount= FarePerDistanceMean* TaxiFare.trip_distance;
				out.collect(TaxiFare);
			}else if(TaxiFare.fare_amount != null && TaxiFare.trip_distance== null){
				TaxiFare.trip_distance= TaxiFare.fare_amount/FarePerDistanceMean;
				out.collect(TaxiFare);
			}else
			{
				Double a=TaxiFare.fare_amount/TaxiFare.trip_distance;
				if (abs(FarePerDistanceMean-a)<8) {
					FarePerDistanceMeanState.update(a);
					out.collect(TaxiFare);
				}
			}
		}

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<Double> descriptor2 =
					new ValueStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(Double.class));
			FarePerDistanceMeanState = getRuntimeContext().getState(descriptor2);
		}
	}
}
