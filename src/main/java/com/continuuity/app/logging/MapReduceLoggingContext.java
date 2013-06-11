package com.continuuity.app.logging;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 *
 */
public class MapReduceLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_MAP_REDUCE_JOB_ID = "mapReduceId";

  /**
   * Constructs the MapReduceLoggingContext
   * @param accountId account id
   * @param applicationId application id
   * @param mapReduceId mapreduce job id
   */
  public MapReduceLoggingContext(final String accountId, final String applicationId, final String mapReduceId) {
    super(accountId, applicationId);
    setSystemTag(TAG_MAP_REDUCE_JOB_ID, mapReduceId);
  }

  @Override
  public String getLogPartition() {
    return super.getLogPartition() + String.format(":%s", getSystemTag(TAG_MAP_REDUCE_JOB_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/mapred-%s", super.getLogPathFragment(), getSystemTag(TAG_MAP_REDUCE_JOB_ID));
  }
}
