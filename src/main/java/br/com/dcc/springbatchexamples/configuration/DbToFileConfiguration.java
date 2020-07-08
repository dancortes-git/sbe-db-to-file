package br.com.dcc.springbatchexamples.configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import br.com.dcc.springbatchexamples.aggregator.CustomerLineAggregator;
import br.com.dcc.springbatchexamples.domain.Customer;
import br.com.dcc.springbatchexamples.domain.mapper.CustomerRowMapper;
import br.com.dcc.springbatchexamples.listener.SimpleChunkListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class DbToFileConfiguration {

	@Bean
	public JdbcPagingItemReader<Customer> dbToFileReader(final DataSource dataSource) {
		final JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(dataSource);
		reader.setFetchSize(10);
		reader.setRowMapper(new CustomerRowMapper());

		final PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
		queryProvider.setSelectClause("id, email, firstName, lastName");
		queryProvider.setFromClause("from customer");
		final Map<String, Order> sortKeys = new HashMap<>(1);
		sortKeys.put("id", Order.ASCENDING);
		queryProvider.setSortKeys(sortKeys);

		reader.setQueryProvider(queryProvider);

		return reader;
	}

	@Bean
	public FlatFileItemWriter<Customer> dbToFileWriter() throws Exception {
		final FlatFileItemWriter<Customer> itemWriter = new FlatFileItemWriter<>();

		itemWriter.setLineAggregator(new CustomerLineAggregator());
		final String customerOutputPath = File.createTempFile("customerOutput", ".out").getAbsolutePath();
		log.info(">> Customer output path: {}", customerOutputPath);
		itemWriter.setResource(new FileSystemResource(customerOutputPath));
		itemWriter.afterPropertiesSet();
		return itemWriter;
	}

	@Bean
	public Step dbToFileStep(final StepBuilderFactory stepBuilderFactory, final DataSource dataSource) throws Exception {
		return stepBuilderFactory.get("DbToFileStep")
			.<Customer, Customer>chunk(50)
			.listener(new SimpleChunkListener())
			.reader(dbToFileReader(dataSource))
			.writer(dbToFileWriter())
			.build();
	}

	@Bean
	public Job dbToFileJob(final JobBuilderFactory jobBuilderFactory, final StepBuilderFactory stepBuilderFactory,
		final DataSource dataSource) throws Exception {
		return jobBuilderFactory.get("DbToFileJob")
				.start(dbToFileStep(stepBuilderFactory, dataSource))
				.build();
	}

}
