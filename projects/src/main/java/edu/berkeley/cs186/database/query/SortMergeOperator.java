package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;
import org.omg.CORBA.INTERNAL;

import java.lang.reflect.Array;
import java.util.*;
import java.lang.*;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator implements Iterator<Record> {
    /* TODO: Implement the SortMergeIterator */
    private String leftTable;
    private String rightTable;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;
    private Page leftPage;
    private Page rightPage;
    private int leftEntryNum;
    private int rightEntryNum;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int rightMark;
    private boolean reset = true;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      if (SortMergeOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTable = ((SequentialScanOperator) SortMergeOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTable = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getLeftColumnName() + "Left";
        SortMergeOperator.this.createTempTable(SortMergeOperator.this.getLeftSource().getOutputSchema(), leftTable);
        Iterator<Record> leftIter = SortMergeOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          SortMergeOperator.this.addRecord(leftTable, leftIter.next().getValues());
        }
      }
      if (SortMergeOperator.this.getRightSource().isSequentialScan()) {
        this.rightTable = ((SequentialScanOperator) SortMergeOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTable = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getRightColumnName() + "Right";
        SortMergeOperator.this.createTempTable(SortMergeOperator.this.getRightSource().getOutputSchema(), rightTable);
        Iterator<Record> rightIter = SortMergeOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          SortMergeOperator.this.addRecord(rightTable, rightIter.next().getValues());
        }
      }

      this.leftIterator = SortMergeOperator.this.getPageIterator(this.leftTable);
      this.rightIterator = SortMergeOperator.this.getPageIterator(this.rightTable);
      this.leftEntryNum = 0;
      this.rightEntryNum = 0;
      this.nextRecord = null;

      if (this.leftIterator.hasNext()) {
        this.leftIterator.next();

      }

      if (this.rightIterator.hasNext()) {
        this.rightIterator.next();
      }

      sortTable();

      this.leftHeader = SortMergeOperator.this.getPageHeader(this.leftTable, this.leftPage);
      this.leftRecord = getNextLeftRecordInPage();

      this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTable, this.rightPage);
      this.rightRecord = getNextRightRecordInPage();
    }

    public void sortTable() throws DatabaseException {
      List<Record> allLeftRecords = new ArrayList<Record>();
      List<Record> allRightRecords = new ArrayList<Record>();

      while (this.leftIterator.hasNext()) {
        this.leftPage = this.leftIterator.next();
        //System.out.println("pageNum: " + this.leftPage.getPageNum());
        this.leftHeader = SortMergeOperator.this.getPageHeader(this.leftTable, this.leftPage);
        this.leftRecord = getNextLeftRecordInPage();
        while (this.leftRecord != null) {
          allLeftRecords.add(this.leftRecord);
          this.leftRecord = getNextLeftRecordInPage();
        }
      }

      while (this.rightIterator.hasNext()) {
        this.rightPage = this.rightIterator.next();
        this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTable, this.rightPage);
        this.rightRecord = getNextRightRecordInPage();
        while (this.rightRecord != null) {
          allRightRecords.add(this.rightRecord);
          this.rightRecord = getNextRightRecordInPage();
        }
      }

      Collections.sort(allLeftRecords, new LeftRecordComparator());
      Collections.sort(allRightRecords, new RightRecordComparator());

      String tempLeftTable = "leftSorted";
      String tempRightTable = "rightSorted";
      SortMergeOperator.this.createTempTable(SortMergeOperator.this.getLeftSource().getOutputSchema(), tempLeftTable);
      SortMergeOperator.this.createTempTable(SortMergeOperator.this.getRightSource().getOutputSchema(), tempRightTable);

      for (Record r : allLeftRecords) {
        SortMergeOperator.this.addRecord(tempLeftTable, r.getValues());
      }

      for (Record r : allRightRecords) {
        SortMergeOperator.this.addRecord(tempRightTable, r.getValues());
      }

      this.leftTable = tempLeftTable;
      this.rightTable = tempRightTable;
      this.leftIterator = SortMergeOperator.this.getPageIterator(this.leftTable);
      this.rightIterator = SortMergeOperator.this.getPageIterator(this.rightTable);
      this.leftEntryNum = 0;
      this.rightEntryNum = 0;
    }


    public int compare(Record left, Record right) {
      DataBox leftValue = left.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
      DataBox rightValue = right.getValues().get(SortMergeOperator.this.getRightColumnIndex());
      return leftValue.compareTo(rightValue);
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

      if (this.leftRecord == null || this.leftPage == null || this.rightPage == null) {
        return false;
      }

      while (this.leftRecord != null) {
        //System.out.println("leftEntryNum: " + this.leftEntryNum);
        //System.out.println("rightEntryNum: " + this.rightEntryNum);
        while (compare(this.leftRecord, this.rightRecord) == -1) {
          this.leftRecord = getNextLeftRecordInPage();
          if (this.leftRecord == null || this.rightRecord == null) {
            return false;
          }
        }

        while (compare(this.leftRecord, this.rightRecord) == 1) {
          this.rightRecord = getNextRightRecordInPage();
          if (this.leftRecord == null || this.rightRecord == null) {
            return false;
          }
        }

        if (this.reset) {
          this.rightMark = this.rightEntryNum;
          this.reset = false;
        }

        while (compare(this.leftRecord, this.rightRecord) == 0) {
          DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            this.rightRecord = getNextRightRecordInPage();
            if (this.rightRecord == null || compare(this.leftRecord, this.rightRecord) != 0) {
              this.reset = true;
              this.rightEntryNum = this.rightMark;
              this.leftRecord = getNextLeftRecordInPage();
            }
            return true;
          }
        }
      }
      return false;
    }

//    private boolean advanceLeftTable() {
//      return false;
//    }
//
//    private boolean advanceRightTable() {
//      return false;
//    }

    private Record getNextLeftRecordInPage() {
      try {
        while (this.leftIterator.hasNext()) {
          if (this.leftEntryNum == SortMergeOperator.this.getNumEntriesPerPage(this.leftTable)) {
            this.leftEntryNum = 0;
            this.leftPage = this.leftIterator.next();
            this.leftHeader = SortMergeOperator.this.getPageHeader(this.leftTable, this.leftPage);
          }
          while (this.leftEntryNum < SortMergeOperator.this.getNumEntriesPerPage(this.leftTable)) {
            byte b = leftHeader[this.leftEntryNum / 8];
            int bitOffset = 7 - (this.leftEntryNum % 8);
            byte mask = (byte) (1 << bitOffset);
            byte value = (byte) (b & mask);
            if (value != 0) {
              int entrySize = SortMergeOperator.this.getEntrySize(this.leftTable);
              int offset = SortMergeOperator.this.getHeaderSize(this.leftTable) + (entrySize * this.leftEntryNum);
              byte[] bytes = this.leftPage.readBytes(offset, entrySize);
              Record r = SortMergeOperator.this.getLeftSource().getOutputSchema().decode(bytes);
              this.leftEntryNum += 1;
              return r;
            }
            this.leftEntryNum += 1;
          }
        }
      } catch (DatabaseException d)  {
        return null;
      }
      return null;
    }

    private Record getNextRightRecordInPage() {
      try {
        while (this.rightIterator.hasNext()) {
          if (this.rightEntryNum == SortMergeOperator.this.getNumEntriesPerPage(this.rightTable)) {
            //System.out.println("Entering new right page");
            this.rightEntryNum = 0;
            this.rightPage = this.rightIterator.next();
            this.rightHeader = SortMergeOperator.this.getPageHeader(this.rightTable, this.rightPage);
          }
          while (this.rightEntryNum < SortMergeOperator.this.getNumEntriesPerPage(this.rightTable)) {
            byte b = rightHeader[this.rightEntryNum / 8];
            int bitOffset = 7 - (this.rightEntryNum % 8);
            byte mask = (byte) (1 << bitOffset);
            byte value = (byte) (b & mask);
            if (value != 0) {
              int entrySize = SortMergeOperator.this.getEntrySize(this.rightTable);
              int offset = SortMergeOperator.this.getHeaderSize(this.rightTable) + (entrySize * rightEntryNum);
              byte[] bytes = this.rightPage.readBytes(offset, entrySize);
              Record r = SortMergeOperator.this.getRightSource().getOutputSchema().decode(bytes);
              this.rightEntryNum += 1;
              return r;
            }
            this.rightEntryNum += 1;
          }
        }
      } catch (DatabaseException d) {
        return null;
      }
      return null;
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


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
