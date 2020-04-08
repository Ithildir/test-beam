package com.github.ithildir.test.beam;

import org.apache.beam.sdk.Pipeline;

public interface PipelineBuilder {

	public Pipeline build(String[] args);

}