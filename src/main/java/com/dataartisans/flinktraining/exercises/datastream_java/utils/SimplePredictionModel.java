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

package com.dataartisans.flinktraining.exercises.datastream_java.utils;

import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * TravelTimePredictionModel provides a very simple regression model to predict the travel time
 * to a destination location depending on the direction and distance of the departure location.
 *
 * The model builds for multiple direction intervals (think of it as north, north-east, east, etc.)
 * a linear regression model (Apache Commons Math, SimpleRegression) to predict the travel time based
 * on the distance.
 *
 * NOTE: This model is not mean for accurate predictions but rather to illustrate Flink's handling
 * of operator state.
 *
 */
public class SimplePredictionModel {
	SimpleRegression model;

	public SimplePredictionModel() {
		model = new SimpleRegression(false);
	}

	public double predictY(double x) {
		double prediction = model.predict(x);

		if (Double.isNaN(prediction)) {
			return -1;
		}
		else {
			return prediction;
		}
	}

	public void refineModel(double x, double y) {
		model.addData(x, y);
	}
}
