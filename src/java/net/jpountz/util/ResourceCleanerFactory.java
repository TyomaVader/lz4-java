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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for obtaining a {@link ResourceCleaner} instance.
 * <p>
 * This is the Java 7/8 implementation using PhantomReference and a daemon thread.
 * On Java 9+, this class is replaced by the version which uses {@code java.lang.ref.Cleaner}.
 * </p>
 */
public final class ResourceCleanerFactory {

  private static final ResourceCleaner INSTANCE = new PhantomReferenceCleaner();

  private ResourceCleanerFactory() {}

  public static ResourceCleaner getCleaner() {
    return INSTANCE;
  }


  private static final class PhantomReferenceCleaner implements ResourceCleaner {

    private final ReferenceQueue<Object> queue = new ReferenceQueue<Object>();
    private final ConcurrentMap<PhantomReference<?>, Runnable> refs =
        new ConcurrentHashMap<PhantomReference<?>, Runnable>();

    PhantomReferenceCleaner() {
      Thread cleanerThread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            try {
              PhantomReference<?> ref = (PhantomReference<?>) queue.remove();
              Runnable action = refs.remove(ref);
              if (action != null) {
                try {
                  action.run();
                } catch (Throwable t) {
                  // Ignore exceptions from cleanup actions
                }
              }
            } catch (InterruptedException e) {
              // Continue polling
            }
          }
        }
      }, "lz4-java-cleaner");
      cleanerThread.setDaemon(true);
      cleanerThread.setPriority(Thread.MAX_PRIORITY - 2);
      cleanerThread.start();
    }

    @Override
    public Cleanable register(Object obj, Runnable action) {
      PhantomCleanable cleanable = new PhantomCleanable(obj, queue, action, refs);
      refs.put(cleanable, action);
      return cleanable;
    }
  }

  private static final class PhantomCleanable extends PhantomReference<Object> implements Cleanable {

    private final ConcurrentMap<PhantomReference<?>, Runnable> refs;
    private volatile boolean cleaned = false;

    PhantomCleanable(Object referent, ReferenceQueue<Object> queue,
                     Runnable action, ConcurrentMap<PhantomReference<?>, Runnable> refs) {
      super(referent, queue);
      this.refs = refs;
    }

    @Override
    public void clean() {
      if (!cleaned) {
        synchronized (this) {
          if (!cleaned) {
            cleaned = true;
            Runnable action = refs.remove(this);
            if (action != null) {
              action.run();
            }
            clear();
          }
        }
      }
    }
  }
}
