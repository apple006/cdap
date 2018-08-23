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

package co.cask.cdap.internal.bootstrap.guice;

import co.cask.cdap.internal.bootstrap.BootstrapConfig;
import co.cask.cdap.internal.bootstrap.BootstrapConfigProvider;
import co.cask.cdap.internal.bootstrap.BootstrapService;
import co.cask.cdap.internal.bootstrap.BootstrapStep;
import co.cask.cdap.internal.bootstrap.FileBootstrapConfigProvider;
import co.cask.cdap.internal.bootstrap.InMemoryBootstrapConfigProvider;
import co.cask.cdap.internal.bootstrap.executor.BootstrapStepExecutor;
import co.cask.cdap.internal.bootstrap.executor.DefaultNamespaceCreator;
import co.cask.cdap.internal.bootstrap.executor.NativeProfileCreator;
import co.cask.cdap.internal.bootstrap.executor.SystemArtifactLoader;
import co.cask.cdap.internal.bootstrap.executor.SystemProfileCreator;
import com.google.gson.JsonObject;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;

import java.util.ArrayList;
import java.util.List;

/**
 * Guice bindings for bootstrap classes. Binds {@link BootstrapService} as a singleton and binds
 * {@link BootstrapStep.Type} to a {@link BootstrapStepExecutor} responsible for executing that type of step.
 * For unit tests, the in memory module should be used, which will create the default namespace and native profile.
 * For actual CDAP instances, the file based module should be used.
 */
public class BootstrapModules {

  public static Module getInMemoryModule() {
    return new BaseModule() {
      @Override
      protected void configure() {
        super.configure();
        List<BootstrapStep> steps = new ArrayList<>();
        steps.add(new BootstrapStep("create default namespace", BootstrapStep.Type.CREATE_DEFAULT_NAMESPACE,
                                    BootstrapStep.RunCondition.ONCE, new JsonObject()));
        steps.add(new BootstrapStep("create native profile", BootstrapStep.Type.CREATE_NATIVE_PROFILE,
                                    BootstrapStep.RunCondition.ONCE, new JsonObject()));
        BootstrapConfig bootstrapConfig = new BootstrapConfig(steps);
        BootstrapConfigProvider inMemoryProvider = new InMemoryBootstrapConfigProvider(bootstrapConfig);
        bind(BootstrapConfigProvider.class).toInstance(inMemoryProvider);
      }
    };
  }

  public static Module getFileBasedModule() {
    return new BaseModule() {
      @Override
      protected void configure() {
        super.configure();
        bind(BootstrapConfigProvider.class).to(FileBootstrapConfigProvider.class);
      }
    };
  }

  private abstract static class BaseModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(BootstrapService.class).in(Scopes.SINGLETON);
      MapBinder<BootstrapStep.Type, BootstrapStepExecutor> mapBinder = MapBinder.newMapBinder(
        binder(), BootstrapStep.Type.class, BootstrapStepExecutor.class);
      mapBinder.addBinding(BootstrapStep.Type.CREATE_DEFAULT_NAMESPACE).to(DefaultNamespaceCreator.class);
      mapBinder.addBinding(BootstrapStep.Type.CREATE_NATIVE_PROFILE).to(NativeProfileCreator.class);
      mapBinder.addBinding(BootstrapStep.Type.CREATE_SYSTEM_PROFILE).to(SystemProfileCreator.class);
      mapBinder.addBinding(BootstrapStep.Type.LOAD_SYSTEM_ARTIFACTS).to(SystemArtifactLoader.class);
    }
  }
}
