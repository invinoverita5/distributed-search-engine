package cis5550.flame;

import java.io.Serializable;
import java.util.Vector;
import java.util.Iterator;
import java.util.List;

public interface FlameRDD {
  public interface StringToIterable extends Serializable {
  	Iterable<String> op(String a) throws Exception;
  };

  public interface StringToPair extends Serializable {
  	FlamePair op(String a) throws Exception;
  };

  public interface StringToPairIterable extends Serializable {
    Iterable<FlamePair> op(String a) throws Exception;
  };

  public interface StringToString extends Serializable {
    String op(String a) throws Exception;
  };

  public interface StringToBoolean extends Serializable {
    boolean op(String a) throws Exception;
  }

  public interface IteratorToIterator extends Serializable {
    Iterator<String> op(Iterator<String> a) throws Exception;
  }

  public String getTableName();

  // count() should return the number of elements in this RDD. 
  // Duplicate elements should be included in the count.

  public int count() throws Exception;

  // saveAsTable() should cause a table with the specified name to appear 
  // in the KVS that contains the data from this RDD. The table should 
  // have a row for each element of the RDD, and the element should be
  // in a column called 'value'; the key can be anything. 

  public void saveAsTable(String tableNameArg) throws Exception;

  // distinct() should return a new RDD that contains the same
  // elements, except that, if the current RDD contains multiple
  // copies of some elements, the new RDD should contain only 
  // one copy of those elements.

  public FlameRDD distinct() throws Exception;

  // destroy() should delete the underlying table in the key-value store.
  // Any future invocations of any method on this RDD should throw an
  // exception.

  public void destroy() throws Exception;

  // take() should return up 'num' elements from the RDD, or all
  // elements, if the RDD contains fewer than 'num' elements.
  // If the RDD contains more than 'num' elements, any subset
  // of size 'num' may be returned.

  public Vector<String> take(int num) throws Exception;

  // fold() should call the provided lambda for each element of the 
  // RDD, with that element as the second argument. In the first
  // invocation, the first argument should be 'zeroElement'; in
  // each subsequent invocation, the first argument should be the
  // result of the previous invocation. The function returns
  // the result of the last invocation, or 'zeroElement' if the 
  // RDD does not contain any elements.

  public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception;

  // collect() should return a list that contains all the elements 
  // in the RDD.

  public List<String> collect() throws Exception;

  // flatMap() should invoke the provided lambda once for each element 
  // of the RDD, and it should return a new RDD that contains all the
  // strings from the Iterables the lambda invocations have returned.
  // It is okay for the same string to appear more than once in the output;
  // in this case, the RDD should contain multiple copies of that string.
  // The lambda is allowed to return null or an empty Iterable.

	public FlameRDD flatMap(StringToIterable lambda) throws Exception;

  // flatMapToPair() is analogous to flatMap(), except that the lambda
  // returns pairs instead of strings, and tha tthe output is a PairRDD
  // instead of a normal RDD.

  public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception;

  // mapToPair() should invoke the provided lambda once for each element
  // of the RDD, and should return a PairRDD that contains all the pairs
  // that the lambda returns. The lambda is allowed to return null, and
  // different invocations can return pairs with the same keys and/or the
  // same values.

  public FlamePairRDD mapToPair(StringToPair lambda) throws Exception;

  // subtract() returns an RDD containing all the elements from this RDD
  // that are not present in the other RDD.
  public FlameRDD subtract(String other) throws Exception;

  // mapPartitions() should invoke the provided lambda once per partition,
  // passing an iterator over all elements in that partition. The lambda
  // returns an iterator of output elements, which are collected into a new RDD.
  public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception;
}
