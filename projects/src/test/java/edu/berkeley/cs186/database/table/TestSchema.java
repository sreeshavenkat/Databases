package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;


import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class TestSchema {
  @Test
  public void testSchemaRetrieve() {
    Schema schema = TestUtils.createSchemaWithAllTypes();

    Record input = TestUtils.createRecordWithAllTypes();
    byte[] encoded = schema.encode(input);
    Record decoded = schema.decode(encoded);
    assertEquals(input, decoded);
  }

  @Test
  public void testValidRecord() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    try {
      Record output = schema.verify(input.getValues());
      assertEquals(input, output);
    } catch (SchemaException se) {
      fail();
    }
  }

  @Test
  @Category(StudentTest.class)
  public void testInvalidRecordInput() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();
    Record input2 = TestUtils.createRecordWithAllTypesWithValue(2);
    try {
      Record output = schema.verify(input2.getValues());
      assertNotEquals(input, output);
    } catch (SchemaException se) {
      fail();
    }
  }


  @Test(expected = SchemaException.class)
  @Category(StudentTest.class)
  public void testWrongType() throws SchemaException {
    Schema schema = TestUtils.createSchemaOfBool();
    List<DataBox> values = new ArrayList<DataBox>();
    values.add(new StringDataBox("fail", 2));
    schema.verify(values);
  }

  @Test(expected = SchemaException.class)
  @Category(StudentTest.class)
  public void testAllTypes() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();
    values.add(new StringDataBox("pass", 2));
    values.add(new IntDataBox(7));
    values.add(new BoolDataBox(false));
    schema.verify(values);
  }

  @Test(expected = SchemaException.class)
  public void testInvalidRecordLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    schema.verify(new ArrayList<DataBox>());
  }

  @Test(expected = SchemaException.class)
  public void testInvalidFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new StringDataBox("abcde", 5));
    values.add(new IntDataBox(10));

    schema.verify(values);
  }

}
