/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.api.metrics;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Query that specifies parameters to delete entries from {@link MetricStore}.
 */
public class MetricDeleteQuery {

  private final long startTs;
  private final long endTs;
  private final Collection<String> metricNames;
  private final LinkedHashMap<String, String> sliceByTagValues;

  public MetricDeleteQuery(long startTs, long endTs, Collection<String> metricNames,
                           LinkedHashMap<String, String> sliceByTagValues) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.metricNames = metricNames;
    this.sliceByTagValues = new LinkedHashMap<>(sliceByTagValues);
  }

  public MetricDeleteQuery(long startTs, long endTs,
                           LinkedHashMap<String, String> sliceByTagValues) {
    this(startTs, endTs, Collections.emptyList(), sliceByTagValues);
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public Collection<String> getMetricNames() {
    return metricNames;
  }

  public LinkedHashMap<String, String> getSliceByTags() {
    return sliceByTagValues;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("startTs", startTs)
      .add("endTs", endTs)
      .add("metricName", metricNames)
      .add("sliceByTags", Joiner.on(",").withKeyValueSeparator(":").useForNull("null").join(sliceByTagValues))
      .toString();
  }
}
