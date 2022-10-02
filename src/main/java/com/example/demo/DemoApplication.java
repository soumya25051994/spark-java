package com.example.demo;

// import org.springframework.boot.SpringApplication;
// import org.springframework.boot.autoconfigure.SpringBootApplication;

// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.SparkConf;
// import org.apache.curator.utils.PathUtils;

// import java.nio.file.InvalidPathException;
// import java.nio.file.Paths;

import java.io.File; // Import the File class
import java.io.FileNotFoundException; // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

// @SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		// SpringApplication.run(DemoApplication.class, args);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Soumya Ranjan Sahoo");

		// SparkConf conf = new SparkConf().setAppName("Simple
		// Application").setMaster("local");
		// JavaSparkContext sc = new JavaSparkContext(conf);

		// String logFile = "src/main/resources/README.txt";
		// JavaRDD<String> logData = sc.textFile(logFile).cache();

		// System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		// System.out.println(logData);
		// System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

		// long numAs = logData.filter(s -> s.contains("a")).count();
		// long numBs = logData.filter(s -> s.contains("b")).count();

		// System.out.println("*****************************************************************");
		// System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		// System.out.println("*****************************************************************");

		System.out.println("Working Directory = " + System.getProperty("user.dir"));

		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();

		String path = "src/main/resources/people.csv";
		// PathUtils.validatePath(path);

		try {
			File myObj = new File(path);
			Scanner myReader = new Scanner(myObj);
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				System.out.println(data);
			}
			myReader.close();

			System.out.println(
					"***************************1111111111111111111111111111111**************************************");
			Dataset<Row> df3 = spark.read().option("delimiter", ";").option("header", "true").csv(path);
			df3.show();
			System.out.println("***************************END**************************************");

		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		Dataset<Row> jdbcDF = spark.read()
				.format("jdbc")
				.option("url", "jdbc:mysql://localhost:3306/information_schema")
				.option("dbtable", "ALL_PLUGINS")
				.option("user", "root")
				.option("password", "123456")
				.load();

		System.out.println("+++++++++++++++++++++++++++++++++++++++++++222222222222222222222+++++++++++++++++++++++++++++++++++");
		System.out.println(jdbcDF);
		// jdbcDF.select(col("code"), col("name"), col("isActive"), col("createdByUser"), col("modifiedByUser")).show();
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++222222222222222222222+++++++++++++++++++++++++++++++++++");
		// jdbcDF.write()
		// .format("jdbc")
		// .option("url", "jdbc:mysql:localhost")
		// .option("dbtable", "information_schema.customer")
		// .option("user", "root")
		// .option("password", "123456")
		// .save();

		spark.stop();

		// spark-submit --class com.example.demo.DemoApplication ..\..\demo\demo\target\demo-0.0.1-SNAPSHOT.jar
		// command prompt open in : C:\Soumya\spark-3.3.0-bin-hadoop3\bin>

		// ..\..\spark-3.3.0-bin-hadoop3\bin\spark-shell --driver-class-path mysql-connector-java-8.0.30.jar --jars mysql-connector-java-8.0.30.jar
		// ..\..\spark-3.3.0-bin-hadoop3\bin\spark-submit --class com.example.demo.DemoApplication target\demo-0.0.1-SNAPSHOT.jar
		// command prompt open in : C:\Soumya\demo\demo>
	}

}
