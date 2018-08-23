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

package co.cask.cdap.internal.bootstrap.executor;

import co.cask.cdap.common.MethodNotAllowedException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.proto.bootstrap.BootstrapStepResult;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import com.google.gson.JsonObject;
import com.google.inject.Injector;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests {@link NativeProfileCreator}.
 */
public class NativeProfileCreatorTest {
  private static NativeProfileCreator nativeProfileCreator;
  private static ProfileService profileService;

  @BeforeClass
  public static void setupClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    nativeProfileCreator = injector.getInstance(NativeProfileCreator.class);
    profileService = injector.getInstance(ProfileService.class);
  }

  @After
  public void cleanupTest() {
    profileService.clear();
  }

  @Test
  public void testCreation() throws NotFoundException {
    try {
      profileService.getProfile(ProfileId.NATIVE);
      Assert.fail("Native profile should not exist.");
    } catch (NotFoundException e) {
      // expected
    }

    BootstrapStepResult result = nativeProfileCreator.execute("label", new JsonObject());
    BootstrapStepResult expected = new BootstrapStepResult("label", BootstrapStepResult.Status.SUCCEEDED);
    Assert.assertEquals(expected, result);
    Assert.assertEquals(Profile.NATIVE, profileService.getProfile(ProfileId.NATIVE));
  }

  @Test
  public void testAlreadyExistsDoesNotError() throws NotFoundException, MethodNotAllowedException {
    profileService.saveProfile(ProfileId.NATIVE, Profile.NATIVE);
    BootstrapStepResult result = nativeProfileCreator.execute("label", new JsonObject());
    BootstrapStepResult expected = new BootstrapStepResult("label", BootstrapStepResult.Status.SUCCEEDED);
    Assert.assertEquals(expected, result);
    Assert.assertEquals(Profile.NATIVE, profileService.getProfile(ProfileId.NATIVE));
  }
}
