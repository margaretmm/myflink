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

package com.dataartisans.flinktraining.exercises.datastream_java.datatypes;

import org.joda.time.DateTime;

import java.io.Serializable;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the type of the event (start or end)
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the taxiId
 * - the driverId
 *
 */
public class DataEnergy implements  Serializable {
	public String date;
	public Integer Appliances;
	public Integer lights;
	public Double T1;
	public Double RH_1;
	public Double T2;
	public Double RH_2;
	public Double T3	;
	public Double RH_3;
	public Double T4	;
	public Double RH_4;
	public Double T5	;
	public Double RH_5;
	public Double T6;
	public Double RH_6;
	public Double T_out;
	public Double Press_mm_hg;
	public Double RH_out;
	public Double Windspeed;
	public Double Visibility;
	public Double Tdewpoint;
	public Double rv1;
	public Double rv2;
	public DateTime eventTime;

	public DataEnergy() {
		this.eventTime = new DateTime();
	}

	public DataEnergy(String date,int Appliances, int lights, Double T1, Double RH_1,
					  Double T2, Double RH_2, Double T3, Double RH_3,
					  Double T4, Double RH_4, Double T5, Double RH_5, Double T6, Double RH_6,
					  Double T_out,Double Press_mm_hg,Double RH_out,Double Windspeed, Double Visibility,
					  Double Tdewpoint,Double rv1,Double rv2) {
		this.date = dateConvetor(date);
		this.eventTime = new DateTime(this.date);
		this.Appliances = Appliances;
		this.lights = lights;
		this.T1 = T1;
		this.RH_1 = RH_1;
		this.T2 = T2;
		this.RH_2 = RH_2;
		this.T3 = T3;
		this.RH_3 = RH_3;
		this.T4 = T4;
		this.RH_4 = RH_4;
		this.T5 = T5;
		this.RH_5 = RH_5;
		this.T6 = T6;
		this.RH_6 = RH_6;
		this.T_out = T_out;
		this.Press_mm_hg = Press_mm_hg;
		this.RH_out = RH_out;
		this.Windspeed = Windspeed;
		this.Visibility = Visibility;
		this.Tdewpoint = Tdewpoint;
		this.rv1 = rv1;
		this.rv2 = rv2;
	}

	private static String dateConvetor(String date){
		String ret=date.replace("/","-");
		return ret;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(date).append(",");
		sb.append(Appliances).append(",");
		sb.append(lights).append(",");
		sb.append(T1).append(",");
		sb.append(RH_1).append(",");
		sb.append(T2).append(",");
		sb.append(RH_2).append(",");
		sb.append(T3).append(",");
		sb.append(RH_3).append(",");
		sb.append(T4).append(",");
		sb.append(RH_4).append(",");
		sb.append(T5).append(",");
		sb.append(RH_5).append(",");
		sb.append(T6).append(",");
		sb.append(RH_6).append(",");
		sb.append(T_out).append(",");
		sb.append(Press_mm_hg).append(",");
		sb.append(RH_out).append(",");
		sb.append(Windspeed).append(",");
		sb.append(Visibility).append(",");
		sb.append(Tdewpoint).append(",");
		sb.append(rv1).append(",");
		sb.append(rv2);

		return sb.toString();
	}

	public static DataEnergy instanceFromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 23) {
			System.out.println("#############have missing values record: " + line+"\n");
			//return null;
			//throw new RuntimeException("Invalid record: " + line);
		}

		DataEnergy energy = new DataEnergy();

		try {
			energy.date = tokens[0].length() > 0 ? dateConvetor(tokens[0]):null;
			energy.Appliances = tokens[1].length() > 0 ? Integer.parseInt(tokens[1]):null;
			energy.lights = tokens[2].length() > 0 ? Integer.parseInt(tokens[2]) : null;
			energy.T1 = tokens[3].length() > 0 ? Double.parseDouble(tokens[3]) : null;
			energy.RH_1 = tokens[4].length() > 0 ? Double.parseDouble(tokens[4]) : null;
			energy.T2 = tokens[5].length() > 0 ? Double.parseDouble(tokens[5]): null;
			energy.RH_2 =tokens[6].length() > 0 ? Double.parseDouble(tokens[6]) : null;
			energy.T3 = tokens[7].length() > 0 ? Double.parseDouble(tokens[7]): null;
			energy.RH_3 =tokens[8].length() > 0 ? Double.parseDouble(tokens[8]) : null;
			energy.T4 = tokens[9].length() > 0 ? Double.parseDouble(tokens[9]): null;
			energy.RH_4 =tokens[10].length() > 0 ? Double.parseDouble(tokens[10]) : null;
			energy.T5 = tokens[11].length() > 0 ? Double.parseDouble(tokens[11]): null;
			energy.RH_5 =tokens[12].length() > 0 ? Double.parseDouble(tokens[12]) : null;
			energy.T6 = tokens[13].length() > 0 ? Double.parseDouble(tokens[13]): null;
			energy.RH_6 =tokens[14].length() > 0 ? Double.parseDouble(tokens[14]) : null;
			energy.T_out = tokens[15].length() > 0 ? Double.parseDouble(tokens[15]): null;
			energy.Press_mm_hg =tokens[16].length() > 0 ? Double.parseDouble(tokens[16]) : null;
			energy.RH_out = tokens[17].length() > 0 ? Double.parseDouble(tokens[17]): null;
			energy.Windspeed =tokens[18].length() > 0 ? Double.parseDouble(tokens[18]) : null;
			energy.Visibility =tokens[19].length() > 0 ? Double.parseDouble(tokens[18]) : null;
			energy.Tdewpoint =tokens[20].length() > 0 ? Double.parseDouble(tokens[19]) : null;
			energy.rv1 =tokens[21].length() > 0 ? Double.parseDouble(tokens[20]) : null;
			energy.rv2 =tokens[22].length() > 0 ? Double.parseDouble(tokens[21]) : null;

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}
		return energy;
	}

	public long getEventTime() {
		return this.eventTime.getMillis();
	}
}
