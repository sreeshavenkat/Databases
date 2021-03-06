package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.databox.*;

import javax.xml.crypto.Data;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * The Schema of a particular table.
 *
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a record conforming to this schema
 */
public class Schema {
  private List<String> fields;
  private List<DataBox> fieldTypes;
  private int size;

  public Schema(List<String> fields, List<DataBox> fieldTypes) {
    assert(fields.size() == fieldTypes.size());

    this.fields = fields;
    this.fieldTypes = fieldTypes;
    this.size = 0;

    for (DataBox dt : fieldTypes) {
      this.size += dt.getSize();
    }
  }

  /**
   * Verifies that a list of DataBoxes corresponds to this schema. A list of
   * DataBoxes corresponds to this schema if the number of DataBoxes in the
   * list equals the number of columns in this schema, and if each DataBox has
   * the same type and size as the columns in this schema.
   *
   * @param values the list of values to check
   * @return a new Record with the DataBoxes specified
   * @throws SchemaException if the values specified don't conform to this Schema
   */
  public Record verify(List<DataBox> values) throws SchemaException {
    List<DataBox> fieldtypes = getFieldTypes();
    boolean flag = true;
    if (values.size() == fieldtypes.size()) {
      for (int i = 0; i < values.size(); i++) {
        DataBox v = values.get(i);
        DataBox f = fieldtypes.get(i);
        if (v.getClass().equals(f.getClass()) && v.getSize() == f.getSize()) {
          continue;
        } else {
          throw new SchemaException("Values in DataBox have different size/type compared to columns in Schema.");
        }
      }
    } else {
        throw new SchemaException("Values in DataBox do not conform to this Schema.");
    }

    Record r = new Record(values);
    return r;
  }

  /**
   * Serializes the provided record into a byte[]. Uses the DataBoxes'
   * serialization methods. A serialized record is represented as the
   * concatenation of each serialized DataBox. This method assumes that the
   * input record corresponds to this schema.
   *
   * @param record the record to encode
   * @return the encoded record as a byte[]
   */
  public byte[] encode(Record record) {
    List<DataBox> values = record.getValues();
    ByteBuffer target = ByteBuffer.allocate(this.size);
    for (int i = 0; i < values.size(); i++) {
      byte[] byte_values = values.get(i).getBytes();
      target.put(byte_values);
    }
    return target.array();
  }

  /**
   * Takes a byte[] and decodes it into a Record. This method assumes that the
   * input byte[] represents a record that corresponds to this schema.
   *
   * @param input the byte array to decode
   * @return the decoded Record
   */
  public Record decode(byte[] input) {
    List<DataBox> fieldtypes = getFieldTypes();
    int from = 0;
    List<DataBox> values = new ArrayList<DataBox>();
    for (int i = 0; i < fieldtypes.size(); i++) {
      int ft_size = fieldtypes.get(i).getSize();
      byte[] range = Arrays.copyOfRange(input, from, from + ft_size);
      from += ft_size;
      if (fieldtypes.get(i).type().equals(DataBox.Types.BOOL)) {
        DataBox db = new BoolDataBox(range);
        values.add(db);
      }
      if (fieldtypes.get(i).type().equals(DataBox.Types.INT)) {
        DataBox db = new IntDataBox(range);
        values.add(db);
      }
      if (fieldtypes.get(i).type().equals(DataBox.Types.FLOAT)) {
        DataBox db = new FloatDataBox(range);
        values.add(db);
      }
      if (fieldtypes.get(i).type().equals(DataBox.Types.STRING)) {
        DataBox db = new StringDataBox(range);
        values.add(db);
      }
    }
    Record r = new Record(values);
    return r;
  }

  public int getEntrySize() {
    return this.size;
  }

  public List<String> getFieldNames() {
    return this.fields;
  }

  public List<DataBox> getFieldTypes() {
    return this.fieldTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Schema)) {
      return false;
    }

    Schema otherSchema = (Schema) other;

    if (this.fields.size() != otherSchema.fields.size()) {
      return false;
    }

    for (int i = 0; i < this.fields.size(); i++) {
      DataBox thisType = this.fieldTypes.get(i);
      DataBox otherType = otherSchema.fieldTypes.get(i);

      if (thisType.type() != otherType.type()) {
        return false;
      }

      if (thisType.type().equals(DataBox.Types.STRING) && thisType.getSize() != otherType.getSize()) {
        return false;
      }
    }

    return true;
  }
}
