package org.example.util;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.List;

public class RowCombiner {

    public Row combine(Row left, Row right) {
        List<Object> leftFields = scala.collection.JavaConverters.seqAsJavaList(left.toSeq());
        List<Object> rightFields = scala.collection.JavaConverters.seqAsJavaList(right.toSeq());
        List<Object> sum = new ArrayList<>();
        sum.addAll(leftFields);
        sum.addAll(rightFields);
        return RowFactory.create(sum.toArray()); // creating row without schema!
                                                 // we might want to use GenericRowWithSchema
    }
}
