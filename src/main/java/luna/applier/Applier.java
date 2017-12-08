package luna.applier;

import luna.common.model.Record;
import luna.common.model.SchemaTable;

import java.util.List;

public interface Applier {
    void apply(final List<Record> records, SchemaTable schemaTable);
    void applyOneByOne(Record record);
}
