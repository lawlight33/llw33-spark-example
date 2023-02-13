package org.example.util;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RowTransformer {

    private final Logger logger;
    private final int indexToTransform;
    private final String transformerInstanceId;

    public RowTransformer(Logger logger, int indexToTransform) {
        this.transformerInstanceId = UUID.randomUUID().toString();
        this.logger = logger;
        this.indexToTransform = indexToTransform;
        logger.info("New instance of {} is created. id: {}", this.getClass().getName(), transformerInstanceId);
    }

    public Row transform(Row row) {
        logger.info("transform method called for: {}. id: {}", row, transformerInstanceId);
        Seq<Object> fields = row.toSeq();
        List<Object> fieldsList = new ArrayList<>(scala.collection.JavaConverters.seqAsJavaList(fields));
        fieldsList.set(indexToTransform, fieldsList.get(indexToTransform) + "_modified!");
        return RowFactory.create(fieldsList.toArray()); // creating row without schema!
                                                        // we might want to use GenericRowWithSchema
    }
}
