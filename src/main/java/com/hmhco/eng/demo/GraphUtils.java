package com.hmhco.eng.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;
import static org.apache.spark.sql.functions.countDistinct;

public class GraphUtils {

    static Dataset<Row> querySections(GraphFrame graphFrame) {
        // Get just the vertices and use this as basis for the
        // right part of a left outer join as some sections may
        // not have students or groups but should still appear

        Dataset<Row> sectionsDf = graphFrame.vertices()
                .where("type = 'Section'")
                .selectExpr("id as sectionId");

        // Now find the the sections that have students
        // using a motif and then aggregate to get counts
        Dataset<Row> sectionsWithStudentsDf = graphFrame.find("(section)-[students]->(student)")
                .where("section.type = 'Section'")
                .where("students.type = 'students'")
                .where("student.type = 'OrgUser'")
                .selectExpr("section.id as sectionId",
                        "student.id as studentId")
                .groupBy("sectionId")
                .agg(countDistinct("studentId").alias("studentCount"));

        // Now find the the sections that have groups
        // using a motif and then aggregate to get counts
        Dataset<Row> sectionsWithGroupsDf = graphFrame.find("(section)-[groups]->(group)")
                .where("section.type = 'Section'")
                .where("groups.type = 'groups'")
                .where("group.type = 'Group'")
                .selectExpr("section.id as sectionId",
                        "group.id as groupId")
                .groupBy("sectionId")
                .agg(countDistinct("groupId").alias("groupCount"));

        // Using the first complete list of sections now join in the
        // list of student counts and the list of group counts using
        // a left outer join
        return sectionsDf
                .join(sectionsWithStudentsDf,
                        sectionsDf.col("sectionId").equalTo(sectionsWithStudentsDf.col("sectionId")),
                        "left_outer")
                .drop(sectionsWithStudentsDf.col("sectionId"))
                .join(sectionsWithGroupsDf,
                        sectionsDf.col("sectionId").equalTo(sectionsWithGroupsDf.col("sectionId")),
                        "left_outer")
                .drop(sectionsWithGroupsDf.col("sectionId"));
    }


}
