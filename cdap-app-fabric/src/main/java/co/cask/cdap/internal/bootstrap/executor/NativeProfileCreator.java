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

import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import com.google.inject.Inject;

/**
 * Creates the native profile if it doesn't exist.
 */
public class NativeProfileCreator extends BaseStepExecutor<EmptyArguments> {
  private final ProfileService profileService;

  @Inject
  NativeProfileCreator(ProfileService profileService) {
    this.profileService = profileService;
  }

  @Override
  public void execute(EmptyArguments arguments) {
    profileService.createIfNotExists(ProfileId.NATIVE, Profile.NATIVE);
  }
}
