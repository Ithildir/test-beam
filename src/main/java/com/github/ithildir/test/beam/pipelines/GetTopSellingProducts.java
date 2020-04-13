package com.github.ithildir.test.beam.pipelines;

import com.github.ithildir.test.beam.PipelineBuilder;

import java.io.Serializable;

import java.util.Comparator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class GetTopSellingProducts implements PipelineBuilder {

	@Override
	public Pipeline build(String[] args) {
		GetTopSellingProductsOptions getTopSellingProductsOptions =
			PipelineOptionsFactory.fromArgs(
				args
			).withValidation(
			).as(
				GetTopSellingProductsOptions.class
			);

		Pipeline pipeline = Pipeline.create(getTopSellingProductsOptions);

		pipeline.apply(
			"ReadLines",
			TextIO.read(
			).from(
				"datasets/" + getTopSellingProductsOptions.getInputFile()
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
			"FindTopSellingProducts",
			Top.of(
				getTopSellingProductsOptions.getTopCount(),
				new KVByValueComparator())
		).apply(
			"FlattenList", Flatten.iterables()
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
				getTopSellingProductsOptions.getOutputDir()
			)
		);

		return pipeline;
	}

	public interface GetTopSellingProductsOptions extends PipelineOptions {

		@Default.String("spikey_sales_weekly.csv")
		@Description("Path of the file to read from")
		public String getInputFile();

		@Default.String("output/top-products")
		@Description("Path of the directory to write to")
		public String getOutputDir();

		@Default.Integer(10)
		@Description("Number of products to return")
		public int getTopCount();

		public void setInputFile(String inputFile);

		public void setOutputDir(String outputDir);

		public void setTopCount(int topNumber);

	}

	@SuppressWarnings("serial")
	private static class KVByValueComparator
		implements Comparator<KV<String, Double>>, Serializable {

		@Override
		public int compare(KV<String, Double> kv1, KV<String, Double> kv2) {
			return Double.compare(kv1.getValue(), kv2.getValue());
		}

	}

}