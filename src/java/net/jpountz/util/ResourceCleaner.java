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

/**
 * A resource cleaner that registers cleanup actions to be run when objects
 * become phantom reachable.
 * <p>
 * This interface abstracts over {@code java.lang.ref.Cleaner} (Java 9+) and
 * a PhantomReference-based fallback for Java 7/8.
 * </p>
 */
public interface ResourceCleaner {

  /**
   * Registers an object and a cleanup action to run when the object becomes
   * phantom reachable (or when {@link Cleanable#clean()} is called explicitly).
   * <p>
   * The cleanup action must not hold a strong reference to the object being
   * registered, otherwise the object will never become phantom reachable.
   * </p>
   *
   * @param obj    the object to monitor
   * @param action the cleanup action to run; must not reference {@code obj}
   * @return a Cleanable that can be used to run the action explicitly
   */
  Cleanable register(Object obj, Runnable action);
}
