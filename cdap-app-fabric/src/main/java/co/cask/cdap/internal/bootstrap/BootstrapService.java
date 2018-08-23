/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.internal.bootstrap;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.bootstrap.executor.BootstrapStepExecutor;
import co.cask.cdap.proto.bootstrap.BootstrapResult;
import co.cask.cdap.proto.bootstrap.BootstrapStepResult;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Performs bootstrap steps.
 */
public class BootstrapService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapService.class);
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.onceEvery(50));
  private final BootstrapConfigProvider bootstrapConfigProvider;
  private final Map<BootstrapStep.Type, BootstrapStepExecutor> executors;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final AtomicBoolean bootstrapping;
  private BootstrapConfig config;
  private ExecutorService executorService;

  @Inject
  BootstrapService(BootstrapConfigProvider bootstrapConfigProvider,
                   Map<BootstrapStep.Type, BootstrapStepExecutor> executors,
                   DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.bootstrapConfigProvider = bootstrapConfigProvider;
    this.config = BootstrapConfig.EMPTY;
    this.executors = Collections.unmodifiableMap(executors);
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   new TransactionSystemClientAdapter(txClient),
                                                                   NamespaceId.SYSTEM,
                                                                   Collections.emptyMap(), null, null)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
    this.bootstrapping = new AtomicBoolean(false);
  }

  @Override
  protected void startUp() {
    LOG.info("Starting {}", getClass().getSimpleName());
    config = bootstrapConfigProvider.getConfig();
    executorService = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("bootstrap-service"));
    executorService.execute(() -> {
      if (isBootstrappedWithRetries()) {
        // if the system is already bootstrapped, skip any bootstrap step that is supposed to only run once
        bootstrap(step -> step.getRunCondition() == BootstrapStep.RunCondition.ONCE);
      } else {
        bootstrap();
      }
    });
    LOG.info("Started {}", getClass().getSimpleName());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}", getClass().getSimpleName());
    // Shutdown the executor, which will issue an interrupt to the running thread.
    // There is only a single daemon thread, so no need to wait for termination
    executorService.shutdownNow();
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  /**
   * @return whether the system is bootstrapped or not
   */
  public boolean isBootstrapped() {
    return Transactionals.execute(transactional, dsContext -> {
      BootstrapDataset bootstrapDataset = BootstrapDataset.get(dsContext, datasetFramework);
      return bootstrapDataset.isBootstrapped();
    });
  }

  /**
   * Execute all steps in the loaded bootstrap config without skipping any of them.
   *
   * @return the result of executing the bootstrap steps.
   * @throws IllegalStateException if bootstrapping is already in progress
   */
  public BootstrapResult bootstrap() {
    return bootstrap(x -> false);
  }

  /**
   * Execute the steps in the loaded bootstrap config.
   *
   * @param shouldSkip predicate that determines whether to skip a step
   * @return the result of executing the bootstrap steps.
   * @throws IllegalStateException if bootstrapping is already in progress
   */
  public BootstrapResult bootstrap(Predicate<BootstrapStep> shouldSkip) {
    List<BootstrapStepResult> results = new ArrayList<>(config.getSteps().size());
    if (!bootstrapping.compareAndSet(false, true)) {
      throw new IllegalStateException("Bootstrap already in progress.");
    }

    for (BootstrapStep step : config.getSteps()) {
      results.add(executeStep(step, shouldSkip));
    }
    bootstrapping.set(false);
    return new BootstrapResult(results);
  }

  private BootstrapStepResult executeStep(BootstrapStep step, Predicate<BootstrapStep> shouldSkip) {
    try {
      step.validate();
    } catch (IllegalArgumentException e) {
      return new BootstrapStepResult(step.getLabel(), BootstrapStepResult.Status.FAILED, e.getMessage());
    }

    if (shouldSkip.test(step)) {
      return new BootstrapStepResult(step.getLabel(), BootstrapStepResult.Status.SKIPPED, null);
    }

    BootstrapStepExecutor executor = executors.get(step.getType());
    if (executor == null) {
      // should not be possible, as deserialization of the file into a BootStrapConfig should have failed
      return new BootstrapStepResult(step.getLabel(), BootstrapStepResult.Status.FAILED,
                                     String.format("Unknown bootstrap step type '%s' for '%s'.",
                                                   step.getType(), step.getLabel()));
    }
    return executor.execute(step.getLabel(), step.getArguments());
  }

  /**
   * Reloads the bootstrap config.
   */
  public void reload() {
    config = bootstrapConfigProvider.getConfig();
  }

  /**
   * Clears bootstrap state. Should only be called in tests.
   */
  @VisibleForTesting
  void clearBootstrapState() {
    Transactionals.execute(transactional, dsContext -> {
      BootstrapDataset bootstrapDataset = BootstrapDataset.get(dsContext, datasetFramework);
      bootstrapDataset.clear();
    });
  }

  private boolean isBootstrappedWithRetries() {
    return Retries.callWithRetries(this::isBootstrapped, RetryStrategies.fixDelay(6, TimeUnit.SECONDS),
      t -> {
        // don't retry if we were interrupted, or if the service is not running
        // normally this is only called when the service is starting, but it can be running in unit test
        State serviceState = state();
        if (serviceState != State.STARTING && serviceState != State.RUNNING) {
          return false;
        }
        if (t instanceof InterruptedException) {
          return false;
        }
        // Otherwise always retry, but log unexpected types of failures
        // We expect things like SocketTimeoutException or ConnectException
        // when talking to Dataset Service during startup
        Throwable rootCause = Throwables.getRootCause(t);
        if (!(rootCause instanceof SocketTimeoutException || rootCause instanceof ConnectException)) {
          SAMPLING_LOG.warn("Error checking bootstrap state. "
                              + "Bootstrap steps will not be run until state can be checked.", t);
        }
        return true;
      });
  }
}
