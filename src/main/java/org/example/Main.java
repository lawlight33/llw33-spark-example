package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.example.models.Person;
import org.example.util.RowCombiner;
import org.example.util.RowTransformer;
import org.h2.tools.Server;
import org.slf4j.Logger;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.apache.spark.sql.functions.*;

public class Main {

    private static Logger logger;
    private static SparkSession sparkSession;
    private static JavaSparkContext javaSparkContext;
    private static SQLContext sqlContext;

    private static final String PERSONS_TXT = "src/main/resources/persons.txt";
    private static final String ADDITIONAL_PERSONS_TXT = "src/main/resources/additional_persons.txt";
    private static final String OUTPUT_DIR = "output_parquet";

    public static void main(String[] args) throws Exception {
        init();
        startDB();

        readTextFileDatasetMapToDataframe();
        readTextSimpleDataframeConvertToJsonDataset();
        readCsvDataframeCastFieldMapToDataset();
        sqlPlayground();
        writeDataset();
        readPartitionedDataset();
        conversionRddToDatasetAndViceVersa();
        removeDuplicates();
        createSimpleDataframe();
        readDatasetFromDb();
        workWithColumns();
        datasetUnion();
        datasetGrouping();
        datasetMapPartitions();
        datasetFlatMap();
        datasetReduce();
        datasetAddColumnByMap();
        pairRdd();
        joinDatasets();
        accumulators();
        executionPlans();
        broadcastVariables();
    }

    private static void readTextFileDatasetMapToDataframe() {
        // read text file into Dataset<String>
        Dataset<String> stringDataset = sparkSession.read().textFile(PERSONS_TXT);

        // remove first line
        String header = stringDataset.first();
        stringDataset = stringDataset.filter((FilterFunction<String>) line -> !line.equals(header));

        // print schema
        StructType schema1 = stringDataset.schema();
        logger.info("stringDataset.schema(): {}", schema1.toString());
        logger.info("stringDataset.showString(): \n{}", stringDataset.showString(100, 0, true));

        // map Dataset<String> to Dataset<Person>
        Dataset<Person> objectDataset = stringDataset.map((MapFunction<String, Person>) line -> {
            String[] parts = line.split(",");
            Person person = new Person();
            person.setPassportNumber(Integer.parseInt(parts[0]));
            person.setFirstName(parts[0]);
            person.setLastName(parts[1]);
            person.setAddress(parts[2]);
            return person;
        }, Encoders.bean(Person.class));
        StructType schema2 = objectDataset.schema();
        logger.info("objectDataset.schema(): {}", schema2.toString());
        logger.info("objectDataset.showString(): \n{}", objectDataset.showString(100, 0, true));

        // map Dataset<Person> to dataframe
        Dataset<Row> dataframe = objectDataset.toDF();
        StructType schema3 = dataframe.schema();
        logger.info("dataframe.schema(): {}", schema3.toString());
        logger.info("dataframe.showString(): \n{}", dataframe.showString(100, 0, true));

    }

    private static void readTextSimpleDataframeConvertToJsonDataset() {
        Dataset<Row> simpleDataframe = sparkSession.read().text(PERSONS_TXT);
        Row header = simpleDataframe.first();
        simpleDataframe = simpleDataframe.filter((FilterFunction<Row>) row -> !row.equals(header));
        logger.info("simpleDataframe.schema(): {}", simpleDataframe.schema().toString());
        logger.info("simpleDataframe.showString(): \n{}", simpleDataframe.showString(100, 0, true));

        Dataset<String> jsonDataset = simpleDataframe.toJSON();
        logger.info("jsonDataset1.showString(): \n{}", jsonDataset.showString(100, 0, true));

        Dataset<Person> personDataset = simpleDataframe.map((MapFunction<Row, Person>) row -> {
            String[] parts = row.getString(0).split(",");
            Person person = new Person();
            person.setPassportNumber(Integer.parseInt(parts[0]));
            person.setFirstName(parts[0]);
            person.setLastName(parts[1]);
            person.setAddress(parts[2]);
            return person;
        }, Encoders.bean(Person.class));
        logger.info("personDataset.schema(): {}", personDataset.schema().toString());
        logger.info("personDataset.showString(): \n{}", personDataset.showString(100, 0, true));

        jsonDataset = personDataset.toJSON();
        logger.info("jsonDataset2.showString(): \n{}", jsonDataset.showString(100, 0, true));
    }

    private static void readCsvDataframeCastFieldMapToDataset() {
        // read CSV to dataframe with header considering
        Dataset<Row> csvDataframe = sparkSession.read().option("header", "true").csv(PERSONS_TXT);
        logger.info("csvDataframe.schema(): {}", csvDataframe.schema().toString());
        logger.info("csvDataframe.showString(): \n{}", csvDataframe.showString(100, 0, true));

        // cast column from String to Integer
        csvDataframe = csvDataframe.withColumn("passportNumber", col("passportNumber").cast("int"));

        // cast dataframe to dataset
        Dataset<Person> personDataset = csvDataframe.as(Encoders.bean(Person.class));
        logger.info("personDataset.schema(): {}", personDataset.schema().toString());
        logger.info("personDataset.showString(): \n{}", personDataset.showString(100, 0, true));
    }

    private static void sqlPlayground() {
        Dataset<Row> dataframe = sparkSession.read().option("header", "true").csv(PERSONS_TXT);
        dataframe.createOrReplaceTempView("persons");
        Dataset<Row> result = sqlContext.sql("select firstName from persons");
        logger.info("result.showString(): \n{}", result.showString(10, 0, true));
    }

    private static void writeDataset() {
        Dataset<Row> dataframe = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        // partitionBy structure. partition columns will be lost into the parquets
        dataframe.write()
                .partitionBy("firstName", "lastName")
                .parquet(OUTPUT_DIR + "/1");

        // repartition 1 structure
        dataframe.repartition(1)
                .write()
                .parquet(OUTPUT_DIR + "/2");

        // max records per file
        dataframe.write()
                .option("maxRecordsPerFile", 2)
                .parquet(OUTPUT_DIR + "/3");

        // repartition & partitionBy structure. partition columns will be lost into the parquets
        dataframe.repartition(2)
                .write()
                .partitionBy("firstName", "lastName")
                .parquet(OUTPUT_DIR + "/4");
    }

    private static void readPartitionedDataset() {
        // read partition, and partition columns will be missing
        Dataset<Row> dataset = sparkSession.read().parquet(OUTPUT_DIR + "/1/firstName=Jack/lastName=Peterson");
        logger.info("dataset1.schema(): {}", dataset.schema().toString());
        logger.info("dataset1.showString(): \n{}", dataset.showString(100, 0, true));

        // read partition, partitions columns will be picked from file structure
        // full scan will not be performed
        dataset = sparkSession.read().parquet(OUTPUT_DIR + "/1");
        dataset.createOrReplaceTempView("persons");
        dataset = sqlContext.sql("select * from persons where firstName='Jack' and lastName='Peterson'");
        logger.info("dataset2.schema(): {}", dataset.schema().toString());
        logger.info("dataset2.showString(): \n{}", dataset.showString(100, 0, true));
    }

    private static void conversionRddToDatasetAndViceVersa() {
        JavaRDD<String> javaRDD = javaSparkContext.textFile(PERSONS_TXT);
        logger.info("javaRDD.collect().toString(): \n{}", javaRDD.collect());

        Dataset<String> dataset = sparkSession.createDataset(javaRDD.rdd(), Encoders.STRING());
        logger.info("dataset.schema(): {}", dataset.schema().toString());
        logger.info("dataset.showString(): \n{}", dataset.showString(100, 0, true));

        javaRDD = dataset.javaRDD();
        logger.info("javaRDD.collect().toString(): \n{}", javaRDD.collect());
    }

    private static void removeDuplicates() {
        // read original dataframe
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);
        logger.info("dataset.schema(): {}", dataset.schema().toString());
        logger.info("dataset.showString(): \n{}", dataset.showString(100, 0, true));

        // read remove fully equal lines
        Dataset<Row> dataset1 = dataset.distinct();
        logger.info("dataset1.schema(): {}", dataset1.schema().toString());
        logger.info("dataset1.showString(): \n{}", dataset1.showString(100, 0, true));

        // remove only lines that contains equal values in columns.
        // when specifying two columns, then all values in those columns must be equals.
        Dataset<Row> dataset2 = dataset.dropDuplicates("passportNumber");
        logger.info("dataset2.schema(): {}", dataset2.schema().toString());
        logger.info("dataset2.showString(): \n{}", dataset2.showString(100, 0, true));
    }

    private static void createSimpleDataframe() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("first", "second", "third"));
        rows.add(RowFactory.create("fourth", "fifth", "sixth"));
        rows.add(RowFactory.create("seventh", "eighth", "ninth"));

        StructType structType = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("field_1", DataTypes.StringType, true),
                DataTypes.createStructField("field_2", DataTypes.StringType, true),
                DataTypes.createStructField("field_3", DataTypes.StringType, true)}
        );

        Dataset<Row> dataframe = sparkSession.createDataFrame(rows, structType);
        logger.info("dataframe.schema(): {}", dataframe.schema().toString());
        logger.info("dataframe.showString(): \n{}", dataframe.showString(100, 0, true));
    }

    private static void readDatasetFromDb() {
        Dataset<Row> dataset = sparkSession.read()
                .option("url", "jdbc:h2:tcp://localhost:9092/nio:~/db")
                .option("dbtable", "person")
                .format("jdbc")
                .load();

        // date will be read at every call
        logger.info("dataset.schema(): {}", dataset.schema().toString());
        logger.info("dataset.showString(): \n{}", dataset.showString(100, 0, true));
    }

    private static void workWithColumns() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);
        dataset = dataset
                .withColumn("name",
                        concat(col("firstName"), lit(" "), col("lastName")))
                .drop("firstName", "lastName");
        logger.info("dataset.showString(): \n{}", dataset.showString(100, 0, true));
    }

    private static void datasetUnion() {
        Dataset<Row> dataset1 = sparkSession.read().option("header", "true").csv(PERSONS_TXT);
        Dataset<Row> dataset2 = sparkSession.read().option("header", "true").csv(ADDITIONAL_PERSONS_TXT);

        // simple union by column positions without duplicates
        // schema will be deduced from first dataframe
        // in our case we have different columns order in our files
        Dataset<Row> dataset = dataset1.union(dataset2);
        logger.info("dataset1.showString(): \n{}", dataset.showString(100, 0, true));

        // same as union()
        dataset = dataset1.unionAll(dataset2);
        logger.info("dataset2.showString(): \n{}", dataset.showString(100, 0, true));

        // union not by column positions, but by column names
        // also we can allow missing columns by passing true
        // they will be populated with nulls
        dataset = dataset1.unionByName(dataset2);
        logger.info("dataset3.showString(): \n{}", dataset.showString(100, 0, true));
    }

    private static void datasetGrouping() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);
        dataset = dataset.withColumn("passportNumber", col("passportNumber").cast("int"));

        Dataset<Row> grouped1 = dataset
                .groupBy("address")
                .sum("passportNumber")
                .withColumnRenamed("sum(passportNumber)", "sum")
                .orderBy(col("sum").desc());
        logger.info("grouped1.showString(): \n{}", grouped1.showString(100, 0, true));

        Dataset<Row> grouped2 = dataset
                .groupBy("address")
                .max("passportNumber");
        logger.info("grouped2.showString(): \n{}", grouped2.showString(100, 0, true));
    }

    private static void datasetMapPartitions() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        // we cannot create RowTransformer outside of a map,
        // because executors and driver are the separate JVM
        dataset = dataset.map((MapFunction<Row, Row>) row -> {
            RowTransformer rowTransformer = new RowTransformer(logger, 0);
            return rowTransformer.transform(row);
        }, dataset.encoder());

        // calling cache method to avoid calling RowTransformer on downstream work
        dataset.cache();

        logger.info("dataset1.showString(): \n{}", dataset.showString(100, 0, true));

        // count dataset partitions
        logger.info("dataset1.rdd().getNumPartitions(): {}", dataset.rdd().getNumPartitions());

        // in case of heavy initializations, we can make those once by each partitions
        // for some reason, all maps from the previous step will be called again
        dataset = dataset.mapPartitions((MapPartitionsFunction<Row, Row>) it -> {
            RowTransformer rowTransformer = new RowTransformer(logger, 1);
            scala.collection.Iterator<Row> scalaIt = JavaConverters.asScalaIterator(it);
            scalaIt = scalaIt.map(rowTransformer::transform);
            return JavaConverters.asJavaIterator(scalaIt);
        }, dataset.encoder());
        logger.info("dataset2.showString(): \n{}", dataset.showString(100, 0, true));
    }

    private static void datasetFlatMap() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        // unfold the rows from one to multiple with flatMap()
        dataset = dataset.flatMap((FlatMapFunction<Row, Row>) row -> {
            Row transformedTow = new RowTransformer(logger, 1).transform(row);
            return Arrays.asList(row, transformedTow).iterator();
        }, dataset.encoder());
        logger.info("dataset.showString(): \n{}", dataset.showString(100, 0, true));
    }

    private static void datasetReduce() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        Row row = dataset.reduce((ReduceFunction<Row>) (acc, r) -> {
            RowCombiner rowCombiner = new RowCombiner();
            return rowCombiner.combine(acc, r);
        });
        logger.info("Reduced row data is: {}", row);
    }

    private static void datasetAddColumnByMap() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        StructField newColumn = DataTypes.createStructField("counter", DataTypes.StringType, true);
        StructType oldSchema = dataset.first().schema();
        StructType newSchema = oldSchema.add(newColumn);
        Encoder<Row> newEncoder = RowEncoder.apply(newSchema);
        logger.info("newSchema: {}", newSchema);

        dataset = dataset.map((MapFunction<Row, Row>) row -> {
            List<Object> fields = new ArrayList<>(JavaConverters.seqAsJavaList(row.toSeq()));
            fields.add("1");
            return new GenericRowWithSchema(fields.toArray(), newSchema);
        }, newEncoder);
        logger.info("dataset.showString(): \n{}", dataset.showString(100, 0, true));
    }

    private static void pairRdd() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        JavaPairRDD<String, Row> javaPairRDD = dataset.javaRDD()
                .mapToPair((PairFunction<Row, String, Row>) row ->
                        new Tuple2<>(UUID.randomUUID().toString(), row));

        JavaRDD<String> keys = javaPairRDD.keys();
        JavaRDD<Row> values = javaPairRDD.values();
        logger.info("keys: {}", keys.collect());
        logger.info("values: {}", values.collect());

        List<Tuple2<String, Row>> list = new ArrayList<>();
        list.add(new Tuple2<>("1", RowFactory.create("123_")));
        list.add(new Tuple2<>("1", RowFactory.create("456")));
        list.add(new Tuple2<>("2", RowFactory.create("1")));
        list.add(new Tuple2<>("2", RowFactory.create("2")));
        list.add(new Tuple2<>("2", RowFactory.create("3")));
        list.add(new Tuple2<>("3", RowFactory.create("987")));
        javaPairRDD = javaSparkContext.parallelizePairs(list);
        logger.info("oldJavaPairRDD: {}", javaPairRDD.collect());

        JavaPairRDD<String, Row> newJavaPairRDD = javaPairRDD.reduceByKey((Function2<Row, Row, Row>)
                (left, right) -> new RowCombiner().combine(left, right));
        logger.info("newJavaPairRDD: {}", newJavaPairRDD.collect());

        JavaPairRDD<String, Iterable<Row>> pairedJavaRDD = javaPairRDD.groupByKey();
        logger.info("pairedJavaRDD: {}", pairedJavaRDD.collect());

        JavaDoubleRDD jdd = keys.mapToDouble((DoubleFunction<String>) df -> 1.2d);
        logger.info("sum is: {}", jdd.sum()); // output is 6, because: 1.2 * keys.size() = 1.2 * 5 = 6.0
    }

    private static void joinDatasets() {
        Dataset<Row> dataset1 = sparkSession.read().option("header", "true").csv(PERSONS_TXT);
        logger.info("dataset1.showString(): \n{}", dataset1.showString(100, 0, true));

        Dataset<Row> dataset2 = sparkSession.read().option("header", "true").csv(ADDITIONAL_PERSONS_TXT);
        logger.info("dataset2.showString(): \n{}", dataset2.showString(100, 0, true));

        // inner join
        Dataset<Row> joined = dataset1.join(dataset2,
                dataset1.col("passportNumber").equalTo(dataset2.col("passportNumber")));
        logger.info("joined1.showString(): \n{}", joined.showString(100, 0, true));

        // left join
        joined = dataset1.join(dataset2,
                dataset1.col("passportNumber").equalTo(dataset2.col("passportNumber")),
                "left");
        logger.info("joined2.showString(): \n{}", joined.showString(100, 0, true));

        // right join
        joined = dataset1.join(dataset2,
                dataset1.col("passportNumber").equalTo(dataset2.col("passportNumber")),
                "right");
        logger.info("joined3.showString(): \n{}", joined.showString(100, 0, true));

        // outer join
        joined = dataset1.join(dataset2,
                dataset1.col("passportNumber").equalTo(dataset2.col("passportNumber")),
                "outer");
        logger.info("joined4.showString(): \n{}", joined.showString(100, 0, true));
    }

    private static void accumulators() throws InterruptedException {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        Dataset<Integer> passports = dataset.map((MapFunction<Row, Integer>) row ->
                Integer.valueOf(row.getAs("passportNumber")),
        Encoders.INT());

        // if we want to use this dataset in the future, we must persist it
        dataset.cache();
        // it breaks the lineage, so in case of disrupted nodes can't recompute from source
        // dataset.checkpoint();

        // accumulators used in order to have some common variable on driver side to work with executors
        LongAccumulator accumulator = javaSparkContext.sc().longAccumulator();
        passports.foreach((ForeachFunction<Integer>) accumulator::add);

        logger.info("Result: {}", accumulator.sum());
    }

    private static void executionPlans() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        // simple - will display the physical plan
        // extended - will display physical and logical plans
        // codegen - will display the java code planned to be executed
        // cost - will display the optimized logical plan and related statistics (if they exist)
        // formatted - will display a split output composed of a nice physical plan outline and a section with each node details
        String plan = dataset.queryExecution().explainString(ExplainMode.fromString("formatted"));
        logger.info("Plan is: {}", plan);

        // shortcut for printing execution plans
        // dataset.explain();

        // also we can enable adaptive queries based on runtime statistics
        // spark.conf.set("spark.sql.adaptive.enabled", "true")
        // but the final adaptive plan will be shown only in UI

    }

    private static void broadcastVariables() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(PERSONS_TXT);

        Broadcast<StringBuilder> sb = javaSparkContext.broadcast(new StringBuilder());
        dataset.foreach((ForeachFunction<Row>) row ->
                sb.getValue().append(row.getAs("firstName").toString())
        );

        logger.info("Result: {}", sb.getValue().toString());
    }

    private static void init() throws IOException {
        FileUtils.deleteDirectory(new File(OUTPUT_DIR));
        sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .config("spark.driver.host","127.0.0.1")
                .config("spark.driver.bindAddress","127.0.0.1")
                .config("spark.sql.shuffle.partitions", 2)
                .config("spark.default.parallelism", 2)
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                .appName("TestApp")
                .getOrCreate();
        javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        sqlContext = new SQLContext(sparkSession);
        logger = sparkSession.sparkContext().log();
    }

    private static void startDB() {
        try {
            logger.info("Starting DB ...");
            int port = 9092;
            Server.createTcpServer("-tcpPort", String.valueOf(port), "-tcpAllowOthers", "-ifNotExists").start();
            logger.info("DB was started successfully in localhost:{}", port);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try(Connection conn = DriverManager.getConnection(
                "jdbc:h2:tcp://localhost:9092/nio:~/db", "", "");
            Statement stmt = conn.createStatement()) {
            String dropSql = "drop table if exists person";
            stmt.executeUpdate(dropSql);
            String createSql = "create table person(" +
                    "passportNumber int, " +
                    "firstName varchar(100), " +
                    "lastName varchar(100), " +
                    "address varchar(100))";
            stmt.executeUpdate(createSql);
            String insertSql = "insert into person values " +
                    "(456, 'Linda', 'Zinkina', 'Moscow')";
            stmt.executeUpdate(insertSql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
