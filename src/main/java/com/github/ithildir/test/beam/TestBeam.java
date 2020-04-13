package com.github.ithildir.test.beam;

import com.github.ithildir.test.beam.pipelines.GetTopSellingProducts;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

public class TestBeam {

	public static void main(String[] args) {
		PipelineBuilder pipelineBuilder = new GetTopSellingProducts();

		Pipeline pipeline = pipelineBuilder.build(args);

		PipelineResult pipelineResult = pipeline.run();

		pipelineResult.waitUntilFinish();
	}

}