package com.fastcampus.pass;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@EnableBatchProcessing
@SpringBootApplication
public class PassBatchApplication {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
//
//	public PassBatchApplication(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
//		this.jobBuilderFactory = jobBuilderFactory;
//		this.stepBuilderFactory = stepBuilderFactory;
//	}

	@Bean
	public Job sendNotificationJob(JobRepository jobRepository) {
		return new JobBuilder("sendNotificationJob", jobRepository)
				.start(sendMailStep())  // mail을 전송하는 step 등록
				.next(sendSnsStep())    // sns를 전송하는 step 등록
				.build();
	}

	@Bean
	public Step sendMailStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new StepBuilder("sendMailStep", jobRepository)
				.<MemberDto, Long>chunk(10, transactionManager)
				.reader(itemReader())
				.process(itemProcessor()) // optional. 필수가 아니다.
				.writer(itemWriter())
				.build();
	}

	private DataSource dataSource; // bean을 주입 받았다는 가정 하에 사용

	// DB에서 회원 정보를 조회하는 ItemReader
	@Bean
	public ItemReader<MemberDto> itemReader() throws Exception {
		return new JdbcPagingItemReaderBuilder<MemberDto>()
				.pageSize(10)
				.dataSource(dataSource)
				.rowMapper(new BeanPropertyRowMapper<>(MemberDto.class)) // 값 매핑이 제대로 되지 않으면 getter 호출 시 빈값("")이 반환될 수 있음
				.queryProvider(getQueryProvider())
				.name("itemReader")
				.build();
	}

	private PagingQueryProvider getQueryProvider() throws Exception {
		var queryProvider = new SqlPagingQueryProviderFactoryBean();
		queryProvider.setDataSource(dataSource);

		queryProvider.setSelectClause("id, name, email"); // 조회할 컬럼
		queryProvider.setFromClause("member");            // 테이블명

		// SqlPagingQueryProviderFactoryBean의 경우, 페이징 처리로 인해 sortKey 설정이 필수적.
		queryProvider.setSortKey("id");

		return queryProvider.getObject();
	}

	@Bean
	public ItemProcessor<MemberDto, Long> itemProcessor() {
		return member -> Long.parseLong(member.getId());
	}

	@Bean
	public ItemWriter<Long> itemWriter() {
		return memberIds -> memberIds.forEach(it -> sendMail(it));
	}

	private void sendMail(Long id) {
		// 메일 전송하는 로직
	}
//	@Bean
//	public Step passStep() {
//		return new StepBuilder("sendMailStep", jobRepository)
//				.<MemberDto, Long>chunk(10, transactionManager)
//				.reader(itemReader())
//				.process(itemProcessor()) // optional. 필수가 아니다.
//				.writer(itemWriter())
//				.build();
//	}
//		return new StepBuilder()
//				.tasklet(new TaskLet() {
//
//				})
//			.tasklet(new Tasklet() {
//				@Override
//				public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
//					System.out.println("Execute PassStep");
//					return RepeatStatus.FINISHED;
//				}
//			}).build();
//
//	}
//		return this.stepBuilderFactory.get("passStep")
//				.tasklet(new Tasklet() {
//					@Override
//					public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
//						System.out.println("Execute PassStep");
//						return RepeatStatus.FINISHED;
//					}
//
//				}).build();
//	}

	public static void main(String[] args) {
		SpringApplication.run(PassBatchApplication.class, args);
	}

}
