/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

/**
 * Representing a queue entry fetched from HBase.
 */
final class HBaseQueueEntry {
  private final byte[] rowKey;
  private final byte[] data;
  private final byte[] state;

  HBaseQueueEntry(byte[] rowKey, byte[] data, byte[] state) {
    this.rowKey = rowKey;
    this.data = data;
    this.state = state;
  }

  byte[] getRowKey() {
    return rowKey;
  }

  byte[] getData() {
    return data;
  }

  byte[] getState() {
    return state;
  }
}
