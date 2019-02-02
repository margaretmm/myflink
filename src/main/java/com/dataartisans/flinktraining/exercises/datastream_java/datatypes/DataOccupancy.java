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
import java.text.SimpleDateFormat;

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
public class DataOccupancy implements  Serializable {
	public String date;
	public Double Temp;
	public Double Humidity;
	public Double Light;
	public Double CO2;
	public Double HumidityRatio	;
	public int Occupancy;
	public DateTime eventTime;
//	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public DataOccupancy() {
		this.eventTime = new DateTime();
	}

	public DataOccupancy(String date,Double Temp, Double Humidity,Double Light, Double CO2, Double HumidityRatio	,
						 int Occupancy) {
		this.date = date;
		this.Temp = Temp;
		this.Humidity = Humidity;
		this.Light = Light;
		this.CO2 = CO2;
		this.HumidityRatio = HumidityRatio;
		this.Occupancy = Occupancy;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(date).append(",");
		sb.append(Temp).append(",");
		sb.append(Humidity).append(",");
		sb.append(Light).append(",");
		sb.append(CO2).append(",");
		sb.append(HumidityRatio).append(",");
		sb.append(Occupancy);

		return sb.toString();
	}

	public static DataOccupancy instanceFromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 7) {
			System.out.println("#############Invalid record: " + line+"\n");
			//return null;
			//throw new RuntimeException("Invalid record: " + line);
		}

		DataOccupancy diag = new DataOccupancy();

		try {
			diag.date = tokens[0].length() > 0 ? tokens[0]:null;
			diag.Temp = tokens[1].length() > 0 ? Double.parseDouble(tokens[1]):null;
			diag.Humidity = tokens[2].length() > 0 ? Double.parseDouble(tokens[2]) : null;
			diag.Light = tokens[3].length() > 0 ? Double.parseDouble(tokens[3]) : null;
			diag.CO2 = tokens[4].length() > 0 ? Double.parseDouble(tokens[4]) : null;
			diag.HumidityRatio = tokens[5].length() > 0 ? Double.parseDouble(tokens[5]) : null;
			diag.Occupancy =tokens[6].length() > 0 ? Integer.parseInt(tokens[6]) : null;

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}
		return diag;
	}

	public long getEventTime() {
		return this.eventTime.getMillis();
	}
}
