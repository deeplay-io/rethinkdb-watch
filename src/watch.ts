import {abortable, delay, race} from 'abort-controller-x';
import {assertNever} from 'assert-never';
import {Changes, Connection, RStream} from 'rethinkdb-ts';

export type WatchOptions<T> = {
  /**
   * Signal that aborts the watch. When aborted, the iterator throws
   * `AbortError`.
   */
  signal?: AbortSignal;
  /**
   * The number of changes the server will buffer between client reads before it
   * starts dropping changes and generates an error. Default is 100000.
   */
  changefeedQueueSize?: number;
  /**
   * The time window to buffer updates for. Default is 10.
   */
  bufferTimeMs?: number;
  /**
   * A function that must return the unique key of a value. Defaults to
   * JSON-stringified primary key.
   */
  key?: (value: T) => string;
};

export type WatchUpdate<T> =
  | WatchAddUpdate<T>
  | WatchChangeUpdate<T>
  | WatchRemoveUpdate<T>;

export type WatchAddUpdate<T> = {
  type: 'add';
  oldVal?: undefined;
  newVal: T;
};

export type WatchChangeUpdate<T> = {
  type: 'change';
  oldVal: T;
  newVal: T;
};

export type WatchRemoveUpdate<T> = {
  type: 'remove';
  oldVal: T;
  newVal?: undefined;
};

/**
 * Watch for real-time updates of query results.
 *
 * Emits updates in batches represented as `Map`, where key is JSON-stringified
 * primary key, and value is the update event.
 *
 * The first emission is the initial query result.
 *
 * Subsequent updates are buffered in time window specified by
 * `options.bufferTimeMs`.
 *
 * Supports querying the whole table, `getAll`, `between` and `filter` queries.
 * `orderBy` queries are not supported and will lead to incorrect results. Can
 * be used with `pluck` or `map` as long as `options.key` function is provided.
 */
export async function* watch<T>(
  query: RStream<T>,
  connection?: Connection,
  options: WatchOptions<T> = {},
): AsyncGenerator<ReadonlyMap<string, WatchUpdate<T>>, void, void> {
  const {
    signal = new AbortController().signal,
    bufferTimeMs = 10,
    changefeedQueueSize,
  } = options;

  let key: (value: T) => string;

  if (options.key != null) {
    key = options.key;
  } else {
    const primaryKey = await abortable(
      signal,
      inferPrimaryKey(query, connection),
    );

    key = value => JSON.stringify(value[primaryKey]);
  }

  function getKey(update: WatchUpdate<T>): string {
    const value = key((update.newVal ?? update.oldVal)!);

    if (typeof value !== 'string') {
      throw new Error(
        `key function must return a string value, got ${JSON.stringify(value)}`,
      );
    }

    return value;
  }

  // see the docstring to `handleDuplicateUpdates`
  const states = new Map<
    string,
    {refCount: number; pendingUpdates: WatchChangeUpdate<T>[]}
  >();

  function handleChanges(
    batch: Map<string, WatchUpdate<T>>,
    changes: Changes<T>,
  ): void {
    let update = makeUpdate(changes);

    if (update == null) {
      return;
    }

    const key = getKey(update);

    update = handleDuplicateUpdates(states, key, update);

    if (update == null) {
      return;
    }

    addToBatch(batch, key, update);
  }

  const cursor = await query
    .changes({
      /**
       * `squash: true` introduces inconsistencies if the document is matched by
       * the query multiple times, where it can filter out changes that we need
       * to correctly handle duplicates in `handleDuplicateUpdates`.
       */
      squash: false,
      changefeedQueueSize,
      includeInitial: true,
      includeStates: true,
    })
    .run(connection);

  try {
    const initialBatch = new Map<string, WatchAddUpdate<T>>();

    while (true) {
      const changes = await abortable(signal, cursor.next());
      handleChanges(initialBatch, changes);

      if (changes.state === 'ready') {
        break;
      }
    }

    yield initialBatch;

    let pendingPromise: Promise<Changes<T>> | undefined;

    while (true) {
      let nextPromise: Promise<Changes<T>>;

      if (pendingPromise) {
        nextPromise = pendingPromise;
        pendingPromise = undefined;
      } else {
        nextPromise = cursor.next();
      }

      const batch = new Map<string, WatchUpdate<T>>();

      const changes = await abortable(signal, nextPromise);
      handleChanges(batch, changes);

      await race(signal, signal => [
        delay(signal, bufferTimeMs),
        invoke(async () => {
          while (true) {
            pendingPromise = cursor.next();

            const changes = await abortable(signal, pendingPromise);
            handleChanges(batch, changes);
          }
        }),
      ]);

      if (batch.size > 0) {
        yield batch;
      }
    }
  } finally {
    await cursor.close();
  }
}

async function inferPrimaryKey<T>(
  query: RStream<T>,
  connection: Connection | undefined,
): Promise<keyof T> {
  const info = (await query.info().run(connection)) as any;

  const primaryKey = info?.primary_key ?? info?.table?.primary_key;

  if (primaryKey == null) {
    throw new Error(
      "Failed to infer primary key from 'selection.info()'. Consider providing 'options.key' function",
    );
  }

  return primaryKey;
}

function makeUpdate<T>(changes: Changes<T>): WatchUpdate<T> | null {
  if (changes.error != null) {
    throw new Error(changes.error);
  }

  if (changes.new_val != null) {
    if (changes.old_val == null) {
      return {
        type: 'add',
        newVal: changes.new_val,
      };
    } else {
      return {
        type: 'change',
        newVal: changes.new_val,
        oldVal: changes.old_val,
      };
    }
  } else {
    if (changes.old_val == null) {
      return null;
    } else {
      return {
        type: 'remove',
        oldVal: changes.old_val,
      };
    }
  }
}

/**
 * Deduplicate updates for the same value.
 * 
 * Duplicates can happen when the same value is matched by the query multiple
 * times. For example, if such document is matched by two multi-index entries,
 * and the one match is removed, we will receive a `remove` change; after the
 * second match is removed, we will receive another `remove` change.
 * 
 * This works by keeping track of the number of references to the value, and:
 * 
 *  - Only emitting the first `add` update.
 *  - Delaying the emission of the `change` updates until the reference count
 *    equals the number of delayed `change` updates. This ensures that we only
 *    emit the last `change` update. This may happen as a result of `change` or
 *    `remove` updates.
 *  - Only emitting the last `remove` update.
 */
function handleDuplicateUpdates<T>(
  states: Map<
    string,
    {refCount: number; pendingUpdates: WatchChangeUpdate<T>[]}
  >,
  key: string,
  update: WatchUpdate<T>,
): WatchUpdate<T> | null {
  let state = states.get(key);

  let skip: boolean;

  if (update.type === 'add') {
    if (state != null) {
      state.refCount++;
      
      skip = true;
    } else {
      state = {
        refCount: 1,
        pendingUpdates: [],
      };
      states.set(key, state);

      skip = false;
    }
  } else if (update.type === 'change') {
    if (state == null) {
      throw new Error(`Unexpected 'change' update for non-existent value`);
    } else {
      state.pendingUpdates.push(update);

      // only count last 'change'
      if (state.pendingUpdates.length === state.refCount) {
        state.pendingUpdates.length = 0;

        skip = false;
      } else {
        skip = true;
      }
    }
  } else if (update.type === 'remove') {
    if (state == null) {
      throw new Error(`Unexpected 'remove' update for non-existent value`);
    } else {
      state.refCount--;

      if (state.pendingUpdates.length === state.refCount) {
        if (state.refCount === 0) {
          states.delete(key);
        } else {
          update = state.pendingUpdates[state.pendingUpdates.length - 1]!;

          state.pendingUpdates.length = 0;
        }

        skip = false;
      } else {
        skip = true;
      }
    }
  } else {
    skip = assertNever(update);
  }

  return skip ? null : update;
}

function addToBatch<T>(
  batch: Map<string, WatchUpdate<T>>,
  key: string,
  update: WatchUpdate<T>,
): void {
  const prevUpdate = batch.get(key);

  if (prevUpdate == null) {
    batch.set(key, update);

    return;
  }

  if (prevUpdate.type === 'add') {
    if (update.type === 'add') {
      throw new Error(`Unexpected 'add' update followed by 'add' update`);
    }

    if (update.type === 'change') {
      batch.set(key, {
        type: 'add',
        newVal: update.newVal,
      });

      return;
    }

    if (update.type === 'remove') {
      batch.delete(key);

      return;
    }
  }

  if (prevUpdate.type === 'change') {
    if (update.type === 'add') {
      throw new Error(`Unexpected 'change' update followed by 'add' update`);
    }

    if (update.type === 'change') {
      batch.set(key, {
        type: 'change',
        oldVal: prevUpdate.oldVal,
        newVal: update.newVal,
      });

      return;
    }

    if (update.type === 'remove') {
      batch.set(key, {
        type: 'remove',
        oldVal: prevUpdate.oldVal,
      });

      return;
    }
  }

  if (prevUpdate.type === 'remove') {
    if (update.type === 'add') {
      batch.set(key, {
        type: 'change',
        oldVal: prevUpdate.oldVal,
        newVal: update.newVal,
      });

      return;
    }

    if (update.type === 'change') {
      throw new Error(`Unexpected 'remove' update followed by 'change' update`);
    }

    if (update.type === 'remove') {
      throw new Error(`Unexpected 'remove' update followed by 'remove' update`);
    }
  }
}

function invoke<R>(fn: () => R): R {
  return fn();
}
