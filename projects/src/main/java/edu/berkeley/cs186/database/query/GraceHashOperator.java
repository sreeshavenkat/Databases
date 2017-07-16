package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private String[] leftPartitions;
    private String[] rightPartitions;
    private Record nextRecord;
    private Record rightRecord;
    private int currPartition;
    private int currListIndex;
    private Map<DataBox, ArrayList<Record>> hashTable;
    private List<DataBox> rightRecordValues;
    private ArrayList<Record> currList;
    private String leftTable;
    private String rightTable;

    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      this.hashTable = new HashMap<DataBox, ArrayList<Record>>();
      currListIndex = 0;
      List<DataBox> values;
      DataBox val;
      int partition;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTable = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTable = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTable);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTable);
        leftPartitions[i] = leftTable;
        rightPartitions[i] = rightTable;
      }
      while (this.leftIterator.hasNext()) {
        values = this.leftIterator.next().getValues();
        val = values.get(GraceHashOperator.this.getLeftColumnIndex());
        partition = val.hashCode() % (numBuffers - 1);
        GraceHashOperator.this.addRecord(leftPartitions[partition], values);
      }
      while (this.rightIterator.hasNext()) {
        values = this.rightIterator.next().getValues();
        val = values.get(GraceHashOperator.this.getRightColumnIndex());
        partition = val.hashCode() % (numBuffers - 1);
        GraceHashOperator.this.addRecord(rightPartitions[partition], values);
      }

      this.currPartition = 0;
      leftIterator = GraceHashOperator.this.getTableIterator(leftPartitions[currPartition]);

      while (leftIterator.hasNext()) {
        Record currRecord = leftIterator.next();
        values = currRecord.getValues();
        val = values.get(GraceHashOperator.this.getLeftColumnIndex());
        if (this.hashTable.containsKey(val)) {
          this.hashTable.get(val).add(currRecord);
        } else {
          ArrayList<Record> newList = new ArrayList<Record>();
          newList.add(currRecord);
          this.hashTable.put(val, newList);
        }
      }

      this.nextRecord = null;
      this.rightIterator = GraceHashOperator.this.getTableIterator(rightPartitions[currPartition]);

      if (this.rightIterator.hasNext()) {
        this.rightRecord = this.rightIterator.next();
        this.rightRecordValues = this.rightRecord.getValues();
        this.currList = this.hashTable.get(this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex()));
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }
      while (true) {
        while (this.currList == null && this.rightIterator.hasNext()) {
          this.rightRecord = this.rightIterator.next();
          this.rightRecordValues = this.rightRecord.getValues();
          this.currListIndex = 0;
          this.currList = this.hashTable.get(this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex()));
        }
        if (this.currList != null) {
          if (this.currListIndex < this.currList.size()) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.currList.get(this.currListIndex).getValues());
            leftValues.addAll(rightRecordValues);
            this.nextRecord = new Record(leftValues);
            this.currListIndex += 1;
            return true;
          } else {
            this.currList = null;
          }
        } else {
          if (this.currPartition < numBuffers - 2)  {
            this.currPartition += 1;
            try {
              this.rightIterator = GraceHashOperator.this.getTableIterator(rightPartitions[currPartition]);
              this.leftIterator = GraceHashOperator.this.getTableIterator(leftPartitions[currPartition]);
            } catch (DatabaseException d){
              return false;
            }
            this.hashTable = new HashMap<DataBox, ArrayList<Record>>();
            while (leftIterator.hasNext()) {
              Record currRecord = leftIterator.next();
              List<DataBox> values = currRecord.getValues();
              DataBox val = values.get(GraceHashOperator.this.getLeftColumnIndex());
              if (this.hashTable.containsKey(val)) {
                this.hashTable.get(val).add(currRecord);
              } else {
                ArrayList<Record> newList = new ArrayList<Record>();
                newList.add(currRecord);
                this.hashTable.put(val, newList);
              }
            }
            this.currList = null;
          } else {
            return false;
          }
        }
      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.hasNext()) {
        Record r = this.nextRecord;
        this.nextRecord = null;
        return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
