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

import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

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
public class myqPower implements  Serializable {
	public String Date;
	public String Time;
	public float Global_active_power;
	public float Global_reactive_power;
	public float Voltage;
	public float Global_intensity;
	public float Sub_metering_1;
	public float Sub_metering_2;
	public float Sub_metering_3;
	public DateTime eventTime;

	public myqPower() {
		this.eventTime = new DateTime();
	}

	public myqPower(String Date, String Time, float Global_active_power, float Global_reactive_power,
                     float Voltage, float Global_intensity, float Sub_metering_1, float Sub_metering_2,
					float Sub_metering_3) {
		this.eventTime = new DateTime();
		this.Date = Date;
		this.Time = Time;
		this.Global_active_power = Global_active_power;
		this.Global_reactive_power = Global_reactive_power;
		this.Voltage = Voltage;
		this.Global_intensity = Global_intensity;
		this.Sub_metering_1 = Sub_metering_1;
		this.Sub_metering_2 = Sub_metering_2;
		this.Sub_metering_3 = Sub_metering_3;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(Date).append(",");
		sb.append(Time).append(",");
		sb.append(Global_active_power).append(",");
		sb.append(Global_reactive_power).append(",");
		sb.append(Voltage).append(",");
		sb.append(Global_intensity).append(",");
		sb.append(Sub_metering_1).append(",");
		sb.append(Sub_metering_2).append(",");
		sb.append(Sub_metering_3).append(",");

		return sb.toString();
	}

	public static myqPower instanceFromString(String line) {

		String[] tokens = line.split(";");
		if (tokens.length != 9) {
			System.out.println("#############Invalid record: " + line+"\n");
			return null;
			//throw new RuntimeException("Invalid record: " + line);
		}

		myqPower power = new myqPower();

		try {
			power.Date = tokens[0];
			power.Time = tokens[1];
			power.Global_active_power = tokens[2].length() > 0 ? Float.parseFloat(tokens[2]) : 0.0f;
			power.Global_reactive_power = tokens[3].length() > 0 ? Float.parseFloat(tokens[3]) : 0.0f;
			power.Voltage = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
			power.Global_intensity = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
			power.Sub_metering_1 =Float.parseFloat(tokens[6]);
			power.Sub_metering_2 = Float.parseFloat(tokens[7]);
			power.Sub_metering_3 = Float.parseFloat(tokens[8]);

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}

		return power;
	}

	public long getEventTime() {
		return this.eventTime.getMillis();
	}
}
