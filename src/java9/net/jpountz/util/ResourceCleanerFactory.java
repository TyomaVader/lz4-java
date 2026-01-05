package net.jpountz.util;

/*
 * Copyright 2020 Adrien Grand and the lz4-java contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.ref.Cleaner;

/**
 * Factory for obtaining a {@link ResourceCleaner} instance.
 * <p>
 * This is the Java 9+ implementation using {@code java.lang.ref.Cleaner}.
 * </p>
 */
public final class ResourceCleanerFactory {

  private static final ResourceCleaner INSTANCE = new CleanerWrapper();

  private ResourceCleanerFactory() {}

  public static ResourceCleaner getCleaner() {
    return INSTANCE;
  }

  private static final class CleanerWrapper implements ResourceCleaner {

    private final Cleaner cleaner = Cleaner.create();

    @Override
    public Cleanable register(Object obj, Runnable action) {
      Cleaner.Cleanable cleanable = cleaner.register(obj, action);
      return new CleanableWrapper(cleanable);
    }
  }

  private static final class CleanableWrapper implements Cleanable {

    private final Cleaner.Cleanable delegate;

    CleanableWrapper(Cleaner.Cleanable delegate) {
      this.delegate = delegate;
    }

    @Override
    public void clean() {
      delegate.clean();
    }
  }
}
