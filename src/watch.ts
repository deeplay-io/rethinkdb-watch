import {Changes, Connection, RSelection} from 'rethinkdb-ts';
import AbortController, {AbortSignal} from 'node-abort-controller';
import {abortable, delay, race} from 'abort-controller-x';

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
  selection: RSelection<T>,
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
      inferPrimaryKey(selection, connection),
    );

    key = value => JSON.stringify(value[primaryKey]);
  }

  function getKey(update: WatchUpdate<T>): string {
    return key((update.newVal ?? update.oldVal)!);
  }

  const cursor = await selection
    .changes({
      squash: true,
      changefeedQueueSize,
      includeInitial: true,
      includeStates: true,
    })
    .run(connection);

  try {
    const initial = new Map<string, WatchUpdate<T>>();

    while (true) {
      const changes = await abortable(signal, cursor.next());
      const update = makeUpdate(changes);

      if (update != null) {
        addToBatch(initial, getKey(update), update);
      }

      if (changes.state === 'ready') {
        break;
      }
    }

    yield initial;

    let pendingPromise: Promise<Changes<T>> | undefined;

    while (true) {
      let nextPromise: Promise<Changes<T>>;

      if (pendingPromise) {
        nextPromise = pendingPromise;
        pendingPromise = undefined;
      } else {
        nextPromise = cursor.next();
      }

      const changes = await abortable(signal, nextPromise);
      const update = makeUpdate(changes);

      if (update == null) {
        continue;
      }

      const batch = new Map<string, WatchUpdate<T>>();

      addToBatch(batch, getKey(update), update);

      await race(signal, signal => [
        delay(signal, bufferTimeMs),
        invoke(async () => {
          while (true) {
            pendingPromise = cursor.next();

            const changes = await abortable(signal, pendingPromise);
            const update = makeUpdate(changes);

            if (update == null) {
              continue;
            }

            addToBatch(batch, getKey(update), update);
          }
        }),
      ]);

      yield batch;
    }
  } finally {
    await cursor.close();
  }
}

async function inferPrimaryKey<T>(
  selection: RSelection<T>,
  connection: Connection | undefined,
): Promise<keyof T> {
  const info = (await selection.info().run(connection)) as any;

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
