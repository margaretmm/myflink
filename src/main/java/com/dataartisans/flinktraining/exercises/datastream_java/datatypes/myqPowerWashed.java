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
public class myqPowerWashed implements  Serializable {
	public int Spring;
	public int summer;
	public int autumn;
	public int winter;

	public int morning;
	public int noon;
	public int afternoon;
	public int evening;
	public int beforeDawn;
	public float Global_active_power;
	public float Global_reactive_power;
	public float Voltage;
	public float Sub_metering_1;
	public float Sub_metering_2;
	public float Sub_metering_3;


	public myqPowerWashed() {
		this.Spring = 0;
		this.summer = 0;
		this.autumn = 0;
		this.winter = 0;
		this.morning = 0;
		this.noon = 0;
		this.afternoon = 0;
		this.evening = 0;
		this.beforeDawn = 0;
		this.Global_active_power = 0;
		this.Global_reactive_power = 0;
		this.Voltage = 0;
		this.Sub_metering_1 = 0;
		this.Sub_metering_2 = 0;
		this.Sub_metering_3 = 0;
	}

	public myqPowerWashed(int Spring, int summer,int autumn,int winter,int morning,int noon,int afternoon,int evening,int beforeDawn,
						  float Global_active_power, float Global_reactive_power,
                          float Voltage, float Sub_metering_1, float Sub_metering_2, float Sub_metering_3) {

		this.Spring = Spring;
		this.summer = summer;
		this.autumn = autumn;
		this.winter = winter;
		this.morning = morning;
		this.noon = noon;
		this.afternoon = afternoon;
		this.evening = evening;
		this.beforeDawn = beforeDawn;
		this.Global_active_power = Global_active_power;
		this.Global_reactive_power = Global_reactive_power;
		this.Voltage = Voltage;
		this.Sub_metering_1 = Sub_metering_1;
		this.Sub_metering_2 = Sub_metering_2;
		this.Sub_metering_3 = Sub_metering_3;
	}



	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(Spring).append(",");
		sb.append(summer).append(",");
		sb.append(autumn).append(",");
		sb.append(winter).append(",");
		sb.append(morning).append(",");
		sb.append(noon).append(",");
		sb.append(afternoon).append(",");
		sb.append(evening).append(",");
		sb.append(beforeDawn).append(",");
		sb.append(Global_active_power).append(",");
		sb.append(Global_reactive_power).append(",");
		sb.append(Voltage).append(",");
		sb.append(Sub_metering_1).append(",");
		sb.append(Sub_metering_2).append(",");
		sb.append(Sub_metering_3).append(",");

		return sb.toString();
	}

	public static myqPowerWashed instanceFromString(String line) {

		String[] tokens = line.split(";");
		if (tokens.length != 15) {
			//return null;
			throw new RuntimeException("Invalid record: " + line);
		}

		myqPowerWashed power = new myqPowerWashed();

		try {
			power.Spring = Integer.parseInt(tokens[0]);
			power.summer = Integer.parseInt(tokens[1]);
			power.autumn = Integer.parseInt(tokens[1]);
			power.winter = Integer.parseInt(tokens[1]);
			power.morning = Integer.parseInt(tokens[1]);
			power.noon = Integer.parseInt(tokens[1]);
			power.afternoon = Integer.parseInt(tokens[1]);
			power.evening = Integer.parseInt(tokens[1]);
			power.beforeDawn = Integer.parseInt(tokens[1]);
			power.Global_active_power = tokens[2].length() > 0 ? Float.parseFloat(tokens[2]) : 0.0f;
			power.Global_reactive_power = tokens[3].length() > 0 ? Float.parseFloat(tokens[3]) : 0.0f;
			power.Voltage = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
			power.Sub_metering_1 =Float.parseFloat(tokens[6]);
			power.Sub_metering_2 = Float.parseFloat(tokens[7]);
			power.Sub_metering_3 = Float.parseFloat(tokens[8]);

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}

		return power;
	}

	// sort by timestamp,
	// putting START events before END events if they have the same timestamp
//	public int compareTo(myqPower other) {
//		if (other == null) {
//			return 1;
//		}
//		int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
//
//		return compareTimes;
//
//	}

//	@Override
//	public boolean equals(Object other) {
//		return other instanceof myqPower &&
//				this.rideId == ((myqPower) other).rideId;
//	}
//
//	@Override
//	public int hashCode() {
//		return (int)this.rideId;
//	}
//

}
