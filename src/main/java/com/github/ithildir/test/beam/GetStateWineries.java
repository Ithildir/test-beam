package com.github.ithildir.test.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;

public class GetStateWineries {

	public static void main(String[] args) {
		GetStateWineriesOptions getStateWineriesOptions =
			PipelineOptionsFactory.fromArgs(
				args
			).withValidation(
			).as(
				GetStateWineriesOptions.class
			);

		String inputFile = getStateWineriesOptions.getInputFile();
		String outputDir = getStateWineriesOptions.getOutputDir();
		String state = getStateWineriesOptions.getState();

		Pipeline pipeline = Pipeline.create(getStateWineriesOptions);

		pipeline.apply(
			"GetWineries",
			TextIO.read(
			).from(
				"datasets/" + inputFile
			)
		).apply(
			"GrepWineriesIn" + getStateWineriesOptions.getState(),
			Filter.by(line -> line.contains(state))
		).apply(
			"WriteToFile",
			TextIO.write(
			).to(
				outputDir
			)
		);

		PipelineResult pipelineResult = pipeline.run();

		pipelineResult.waitUntilFinish();
	}

	public interface GetStateWineriesOptions extends PipelineOptions {

		@Default.String("spikey_winery_list.csv")
		@Description("Path of the file to read from")
		public String getInputFile();

		@Default.String("output/result")
		@Description("Path of the file to write to")
		public String getOutputDir();

		@Default.String("California")
		@Description("State to filter")
		public String getState();

		public void setInputFile(String inputFile);

		public void setOutputDir(String outputDir);

		public void setState(String state);

	}

}