package com.hmhco.eng.demo;

import com.google.common.collect.ImmutableList;
import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class GraphUtilsTest extends JavaDataFrameSuiteBase {

    private GraphFrame graphFrame;

    @Before
    public void setUp() {
        DataFrameReader dataFrameReader = impSqlContext().read().option("header", "true");
        Dataset<Row> vertices = dataFrameReader.csv( "src/test/resources/vertices.csv");
        Dataset<Row> edges = dataFrameReader.csv("src/test/resources/edges.csv");

        vertices.show(10, false);
        edges.show(10, false);

        graphFrame = GraphFrame.apply(vertices, edges);
    }

    @Test
    public void testSections() {
        Dataset<Row> actualDf = GraphUtils.querySections(graphFrame);

        /*
        +--------------------+------------+----------+
        |sectionId           |studentCount|groupCount|
        +--------------------+------------+----------+
        |SECTION1            |4           |1         |
        |SECTION2_NO_STUDENTS|null        |null      |
        +--------------------+------------+----------+
         */

        ImmutableList.Builder<StructField> structFieldBuilder =
                getStructFieldBuilder("sectionId");
        structFieldBuilder.add(getStructFieldLong("studentCount"));
        structFieldBuilder.add(getStructFieldLong("groupCount"));
        StructType expectedSchema =
                new StructType(structFieldBuilder.build().toArray(new StructField[0]));
        List<Row> expectedData = new ArrayList<>();
        expectedData.add(RowFactory.create("SECTION1", 4L, 1L));
        expectedData.add(RowFactory.create("SECTION2_NO_STUDENTS", null, null));


        validateDataFrame(expectedData, expectedSchema, actualDf);
    }


    private StructType buildStructFieldList(String... elements) {
        ImmutableList.Builder<StructField> structFieldBuilder = getStructFieldBuilder(elements);
        return new StructType(structFieldBuilder.build().toArray(new StructField[elements.length]));
    }

    private ImmutableList.Builder<StructField> getStructFieldBuilder(String... elements) {
        ImmutableList.Builder<StructField> structFieldBuilder = new ImmutableList.Builder<>();
        for (String type : elements) {
            structFieldBuilder.add(new StructField(type, StringType, true, Metadata.empty()));
        }
        return structFieldBuilder;
    }


    private StructField getStructFieldLong(String name) {
        return new StructField(name, LongType, true, Metadata.empty());
    }


    private void validateDataFrame(List<Row> expectedData, StructType expectedSchema,
            Dataset<Row> actualDf) {
        Dataset<Row> expectedDF = impSqlContext().createDataFrame(expectedData, expectedSchema);

        actualDf.show(10, false);
        expectedDF.show(10, false);

        assertDataFrameEquals(actualDf, expectedDF);
    }
}
