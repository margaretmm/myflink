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
public class DataDiagnosis implements  Serializable {
	public int Temp;
	public int what;
	public String nausea;
	public String Lumbar_pain;
	public String Urine_pushing;
	public String Micturition_pains;
	public String Burning_urethra;
	public String decision_urinary	;
	public String decision_renal_pelvis;
	public DateTime eventTime;

	public DataDiagnosis() {
		this.eventTime = new DateTime();
	}

	public DataDiagnosis(int Temp, int what, String nausea, String Lumbar_pain,
						 String Urine_pushing, String Micturition_pains, String Burning_urethra, String decision_urinary,
						 String decision_renal_pelvis) {
		this.eventTime = new DateTime();
		this.Temp = Temp;
		this.what = what;
		this.nausea = nausea;
		this.Lumbar_pain = Lumbar_pain;
		this.Urine_pushing = Urine_pushing;
		this.Micturition_pains = Micturition_pains;
		this.Burning_urethra = Burning_urethra;
		this.decision_urinary = decision_urinary;
		this.decision_renal_pelvis = decision_renal_pelvis;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(Temp).append(",");
		sb.append(what).append(",");
		sb.append(nausea).append(",");
		sb.append(Lumbar_pain).append(",");
		sb.append(Urine_pushing).append(",");
		sb.append(Micturition_pains).append(",");
		sb.append(Burning_urethra).append(",");
		sb.append(decision_urinary).append(",");
		sb.append(decision_renal_pelvis);

		return sb.toString();
	}

	public static DataDiagnosis instanceFromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 9) {
			System.out.println("#############Invalid record: " + line+"\n");
			return null;
			//throw new RuntimeException("Invalid record: " + line);
		}

		DataDiagnosis diag = new DataDiagnosis();

		try {
			diag.Temp = tokens[0].length() > 0 ? Integer.parseInt(tokens[0]):null;
			diag.what = tokens[1].length() > 0 ? Integer.parseInt(tokens[1]):null;
			diag.nausea = tokens[2].length() > 0 ? tokens[2] : null;
			diag.Lumbar_pain = tokens[3].length() > 0 ? tokens[3] : null;
			diag.Urine_pushing = tokens[4].length() > 0 ? tokens[4] : null;
			diag.Micturition_pains = tokens[5].length() > 0 ? tokens[5] : null;
			diag.Burning_urethra =tokens[6].length() > 0 ? tokens[6] : null;
			diag.decision_urinary = tokens[7].length() > 0 ? tokens[7] : null;
			diag.decision_renal_pelvis = tokens[8].length() > 0 ? tokens[8] : null;

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}
		return diag;
	}

	public long getEventTime() {
		return this.eventTime.getMillis();
	}
}
