package com.github.ithildir.test.beam.pipelines;

import com.github.ithildir.test.beam.PipelineBuilder;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class GetProductRevenue implements PipelineBuilder {

	@Override
	public Pipeline build(String[] args) {
		GetProductRevenueOptions getProductRevenueOptions =
			PipelineOptionsFactory.fromArgs(
				args
			).withValidation(
			).as(
				GetProductRevenueOptions.class
			);

		Pipeline pipeline = Pipeline.create(getProductRevenueOptions);

		pipeline.apply(
			"ReadLines",
			TextIO.read(
			).from(
				"datasets/" + getProductRevenueOptions.getInputFile()
			)
		).apply(
			"ExtractProductRevenue",
			MapElements.into(
				TypeDescriptors.kvs(
					TypeDescriptors.strings(), TypeDescriptors.doubles())
			).via(
				line -> {
					String[] tokens = line.split(",");

					String product = tokens[1];
					double quantity = Double.parseDouble(tokens[2]);
					double price = Double.parseDouble(tokens[3]);

					return KV.of(product, quantity * price);
				}
			)
		).apply(
			"CalculateTotalRevenuePerProduct", Combine.perKey(Sum.ofDoubles())
		).apply(
			"ConvertToStrings",
			MapElements.into(
				TypeDescriptors.strings()
			).via(
				kv -> kv.getKey() + "," + kv.getValue()
			)
		).apply(
			"WiteToFile",
			TextIO.write(
			).to(
				getProductRevenueOptions.getOutputDir()
			)
		);

		return pipeline;
	}

	public interface GetProductRevenueOptions extends PipelineOptions {

		@Default.String("spikey_sales_weekly.csv")
		@Description("Path of the file to read from")
		public String getInputFile();

		@Default.String("output/product-revenue")
		@Description("Path of the directory to write to")
		public String getOutputDir();

		public void setInputFile(String inputFile);

		public void setOutputDir(String outputDir);

	}

}