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
public class DataHeart implements  Serializable {
	public Integer age;
	public Integer sex;
	public Integer chestPain;
	public Integer bloodPressure;
	public Integer serumCholestoral;
	public Integer bloodSugar;
	public Integer electrocardiographic;
	public Integer maximumHeartRate;
	public Integer angina;
	public Float oldpeak;
	public Integer peakExerciseSTSegment;
	public Integer numberOfMajorVessels;
	public Integer thal;
	public Integer target;
	public DateTime eventTime;

	public DataHeart() {
		this.eventTime = new DateTime();
	}

	public DataHeart(int age, int sex, int chestPain, int bloodPressure, int serumCholestoral, int bloodSugar, int electrocardiographic,
					 int maximumHeartRate, int angina, Float oldpeak, int peakExerciseSTSegment, int numberOfMajorVessels, int thal, int target) {
		this.eventTime = new DateTime();
		this.age = age;
		this.sex = sex;
		this.chestPain = chestPain;
		this.bloodPressure = bloodPressure;
		this.serumCholestoral = serumCholestoral;
		this.bloodSugar = bloodSugar;
		this.electrocardiographic = electrocardiographic;
		this.maximumHeartRate = maximumHeartRate;
		this.angina = angina;
		this.oldpeak = oldpeak;
		this.peakExerciseSTSegment = peakExerciseSTSegment;
		this.numberOfMajorVessels = numberOfMajorVessels;
		this.thal = thal;
		this.target = target;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(age).append(",");
		sb.append(sex).append(",");
		sb.append(chestPain).append(",");
		sb.append(bloodPressure).append(",");
		sb.append(serumCholestoral).append(",");
		sb.append(bloodSugar).append(",");
		sb.append(electrocardiographic).append(",");
		sb.append(maximumHeartRate).append(",");
		sb.append(angina).append(",");
		sb.append(oldpeak).append(",");
		sb.append(peakExerciseSTSegment).append(",");
		sb.append(numberOfMajorVessels).append(",");
		sb.append(thal).append(",");
		sb.append(target);

		return sb.toString();
	}

	public static DataHeart instanceFromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 14) {
			System.out.println("#############Invalid record: " + line+"\n");
			return null;
			//throw new RuntimeException("Invalid record: " + line);
		}

		DataHeart diag = new DataHeart();

		try {
			diag.age = tokens[0].length() > 0 ? Integer.parseInt(tokens[0]):null;
			diag.sex = tokens[1].length() > 0 ? Integer.parseInt(tokens[1]):null;
			diag.chestPain = tokens[2].length() > 0 ? Integer.parseInt(tokens[2]) : null;
			diag.bloodPressure = tokens[3].length() > 0 ? Integer.parseInt(tokens[3]) : null;
			diag.serumCholestoral = tokens[4].length() > 0 ? Integer.parseInt(tokens[4]) : null;
			diag.bloodSugar = tokens[5].length() > 0 ? Integer.parseInt(tokens[5]) : null;
			diag.electrocardiographic =tokens[6].length() > 0 ? Integer.parseInt(tokens[6]) : null;
			diag.maximumHeartRate = tokens[7].length() > 0 ? Integer.parseInt(tokens[7]) : null;
			diag.angina = tokens[8].length() > 0 ? Integer.parseInt(tokens[8]) : null;
			diag.oldpeak = tokens[9].length() > 0 ? Float.parseFloat(tokens[9]) : null;
			diag.peakExerciseSTSegment = tokens[10].length() > 0 ? Integer.parseInt(tokens[10]) : null;
			diag.numberOfMajorVessels = tokens[11].length() > 0 ? Integer.parseInt(tokens[11]) : null;
			diag.thal = tokens[12].length() > 0 ? Integer.parseInt(tokens[12]) : null;
			diag.target = tokens[13].length() > 0 ? Integer.parseInt(tokens[13]) : null;


		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}
		return diag;
	}

	public long getEventTime() {
		return this.eventTime.getMillis();
	}
}
