import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionCodec;
import com.google.common.base.Throwables;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Triple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 *
 */
public class TransactionAwareHTable implements HTableInterface, TransactionAware {
  private Transaction tx;
  private HTable hTable;
  private final TransactionCodec txCodec;
  private ArrayList<Triple<byte[], byte[], byte[]>> changeSet;
  private boolean allowNonTransactional;

  public TransactionAwareHTable(HTable hTable) {
    this.hTable = hTable;
    this.changeSet = new ArrayList<Triple<byte[], byte[], byte[]>>();
    this.txCodec = new TransactionCodec();
    this.allowNonTransactional = false;
  }

  public TransactionAwareHTable(HTable hTable, boolean allowNonTransactional) {
    this.hTable = hTable;
    this.changeSet = new ArrayList<Triple<byte[], byte[], byte[]>>(
    );
    this.txCodec = new TransactionCodec();
    this.allowNonTransactional = allowNonTransactional;
  }

  public boolean getAllowNonTransactional() {
    return this.allowNonTransactional;
  }

  public void setAllowNonTransactional(boolean allowNonTransactional) {
    this.allowNonTransactional = allowNonTransactional;
  }

  @Override
  public byte[] getTableName() {
    return hTable.getTableName();
  }

  @Override
  public Configuration getConfiguration() {
    return hTable.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return hTable.getTableDescriptor();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    return hTable.exists(transactionalizeAction(get));
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
    // TODO
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    // TODO
    return new Object[0];
  }

  @Override
  public Result get(Get get) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    return hTable.get(transactionalizeAction(get));
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    ArrayList<Get> transactionalizedGets = new ArrayList<Get>();
    for (Get get : gets) {
      transactionalizedGets.add(transactionalizeAction(get));
    }
    return hTable.get(transactionalizedGets);
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    return hTable.getRowOrBefore(row, family);
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    return hTable.getScanner(transactionalizeAction(scan));
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    Scan scan = new Scan();
    scan.addFamily(family);
    return hTable.getScanner(transactionalizeAction(scan));
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return hTable.getScanner(transactionalizeAction(scan));
  }

  @Override
  public void put(Put put) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    Put txPut = transactionalizeAction(put);
    hTable.put(txPut);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    ArrayList<Put> transactionalizedPuts = new ArrayList<Put>();
    for (Put put : puts) {
      Put txPut = transactionalizeAction(put);
      transactionalizedPuts.add(txPut);
    }
    hTable.put(transactionalizedPuts);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
    if (allowNonTransactional) {
      return hTable.checkAndPut(row, family, qualifier, value, put);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public void delete(Delete delete) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    put(transactionalizeAction(delete));
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    ArrayList<Put> transactionalizedPuts = new ArrayList<Put>();
    for (Delete delete : deletes) {
      Put txPut = transactionalizeAction(delete);
      transactionalizedPuts.add(txPut);
    }
    hTable.put(transactionalizedPuts);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
    throws IOException {
    if (allowNonTransactional) {
      return hTable.checkAndDelete(row, family, qualifier, value, delete);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    if (allowNonTransactional) {
      hTable.mutateRow(rm);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public Result append(Append append) throws IOException {
    if (allowNonTransactional) {
      return hTable.append(append);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    if (allowNonTransactional) {
      return hTable.increment(increment);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
    if (allowNonTransactional) {
      return hTable.incrementColumnValue(row, family, qualifier, amount);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
    throws IOException {
    if (allowNonTransactional) {
      return hTable.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public boolean isAutoFlush() {
    return hTable.isAutoFlush();
  }

  @Override
  public void flushCommits() throws IOException {
    hTable.flushCommits();
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  @Override
  public RowLock lockRow(byte[] row) throws IOException {
    return hTable.lockRow(row);
  }

  @Override
  public void unlockRow(RowLock rl) throws IOException {
    hTable.unlockRow(rl);
  }

  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
    return hTable.coprocessorProxy(protocol, row);
  }

  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol, byte[] startKey,
                                                                           byte[] endKey, Batch.Call<T,R> callable)
    throws IOException, Throwable {
    return hTable.coprocessorExec(protocol, startKey, endKey, callable);
  }

  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol, byte[] startKey, byte[] endKey,
                                                                 Batch.Call<T, R> callable, Batch.Callback<R> callback)
    throws IOException, Throwable {
    hTable.coprocessorExec(protocol, startKey, endKey, callable, callback);
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    hTable.setAutoFlush(autoFlush);
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    hTable.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  @Override
  public long getWriteBufferSize() {
    return hTable.getWriteBufferSize();
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    hTable.setWriteBufferSize(writeBufferSize);
  }

  @Override
  public void startTx(Transaction tx) {
    this.tx = tx;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    Collection transactionChanges = null;
    try {
      transactionChanges = Collections.singletonList(txCodec.encode(tx));
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    return transactionChanges;
  }

  @Override
  public boolean commitTx() throws Exception {
    hTable.flushCommits();
    return true;
  }

  @Override
  public void postTxCommit() {
    tx = null;
    changeSet.clear();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    try {
      for (Triple<byte[], byte[], byte[]> change : changeSet) {
        byte[] row = change.getFirst();
        byte[] family = change.getSecond();
        byte[] qualifier = change.getThird();
        long transactionTimestamp = tx.getWritePointer();
        Delete rollbackDelete = new Delete(row, transactionTimestamp);
        if (family != null && qualifier == null) {
          rollbackDelete.deleteFamily(family, transactionTimestamp);
        } else if (family != null && qualifier != null) {
          rollbackDelete.deleteColumn(family, qualifier, transactionTimestamp);
        }
        hTable.delete(rollbackDelete);
      }
      return true;
    } catch (Exception e) {
      Throwables.propagate(e);
      return false;
    } finally {
      hTable.flushCommits();
      tx = null;
      changeSet.clear();
    }
  }

  @Override
  public String getTransactionAwareName() {
    return Bytes.toString(getTableName());
  }

  // Helpers to get copies of objects with the timestamp set to the current transaction timestamp.

  private Get transactionalizeAction(Get get) throws IOException {
    txCodec.addToOperation(get, tx);
    return get;
  }

  private Scan transactionalizeAction(Scan scan) throws IOException {
    txCodec.addToOperation(scan, tx);
    return scan;
  }

  private Put transactionalizeAction(Put put) throws IOException {
    Put txPut = new Put(put.getRow(), tx.getWritePointer());
    Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
    if (familyMap.isEmpty()) {
      changeSet.add(new Triple(txPut.getRow(), null, null));
    } else {
      for (Map.Entry<byte [], List<KeyValue>> family : familyMap) {
        List<KeyValue> familyValues = family.getValue();
        if (familyValues.isEmpty()) {
          changeSet.add(new Triple(txPut.getRow(), family.getKey(), null));
        } else {
          for (KeyValue value : family.getValue()) {
            txPut.add(value.getFamily(), value.getQualifier(), tx.getWritePointer(), value.getValue());
            changeSet.add(new Triple(txPut.getRow(), value.getFamily(), value.getQualifier()));
          }
        }
      }
    }
    for (Map.Entry<String, byte[]> entry : put.getAttributesMap().entrySet()) {
      txPut.setAttribute(entry.getKey(), entry.getValue());
    }
    txPut.setWriteToWAL(put.getWriteToWAL());
    txCodec.addToOperation(txPut, tx);
    return txPut;
  }

  private Put transactionalizeAction(Delete delete) throws IOException {
    long transactionTimestamp = tx.getWritePointer();
    Result result = get(new Get(delete.getRow()));

    byte[] deleteRow = delete.getRow();
    Put txPut = new Put(deleteRow, transactionTimestamp);

    Map<byte[], List<KeyValue>> familyToDelete = delete.getFamilyMap();
    if (familyToDelete.isEmpty()) {
      // Delete everything
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = result.getNoVersionMap();
      for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> familyEntry : resultMap.entrySet()) {
        NavigableMap<byte[], byte[]> familyColumns = result.getFamilyMap(familyEntry.getKey());
        for (Map.Entry<byte[], byte[]> column : familyColumns.entrySet()) {
          txPut.add(familyEntry.getKey(), column.getKey(), transactionTimestamp, new byte[0]);
        }
      }
    } else {
      for (Map.Entry<byte [], List<KeyValue>> familyEntry : familyToDelete.entrySet()) {
        byte[] family = familyEntry.getKey();
        List<KeyValue> entries = familyEntry.getValue();
        if (entries.isEmpty()) {
          // Delete entire family
          NavigableMap<byte[], byte[]> familyColumns = result.getFamilyMap(family);
          for (Map.Entry<byte[], byte[]> column : familyColumns.entrySet()) {
            txPut.add(family, column.getKey(), new byte[0]);
          }
        } else {
          for (KeyValue value : entries) {
            txPut.add(value.getFamily(), value.getQualifier(), transactionTimestamp, new byte[0]);
          }
        }
      }
    }
    return txPut;
  }
}
