# rethinkdb-watch [![npm version][npm-image]][npm-url] <!-- omit in toc -->

Consume RethinkDB Changefeeds as Async Iterables

- [Installation](#installation)
- [API](#api)
  - [`watch`](#watch)
  - [`consumeFeed`](#consumefeed)

## Installation

```
npm install rethinkdb-watch
```

## API

### `watch`

```ts
import {Connection, RSelection} from 'rethinkdb-ts';

function watch<T>(
  selection: RSelection<T>,
  connection?: Connection,
  options?: WatchOptions<T>,
): AsyncIterable<ReadonlyMap<string, WatchUpdate<T>>>;

type WatchUpdate<T> =
  | {type: 'add'; oldVal?: undefined; newVal: T}
  | {type: 'change'; oldVal: T; newVal: T}
  | {type: 'remove'; oldVal: T; newVal?: undefined};
```

Watch for real-time updates of query results.

Emits updates in batches represented as `Map`, where key is JSON-stringified
primary key, and value is the update event.

The first emission is the initial query result.

Subsequent updates are buffered in time window specified by
`options.bufferTimeMs`.

Supports querying the whole table, `getAll`, `between` and `filter` queries.
`orderBy` queries are not supported and will lead to incorrect results. Can be
used with `pluck` or `map` as long as `options.key` function is provided.

Supported options:

```ts
type WatchOptions<T> = {
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
```

#### Examples <!-- omit in toc -->

Async generator that emits live query states:

```ts
import {r} from 'rethinkdb-ts';
import {watch} from 'rethinkdb-watch';
import {AbortSignal} from 'node-abort-controller';

const connection = await r.connect({host, port});

const query = r
  .db('test')
  .table<Doc>('test')
  .filter(row => row('test'));

async function* watchQueryState(signal: AbortSignal): AsyncGenerator<Doc[]> {
  const state = new Map<string, Doc>();

  for await (const updates of watch(query, connection, {signal})) {
    for (const [key, update] of updates) {
      if (update.newVal != null) {
        state.set(key, update.newVal);
      } else {
        state.delete(key);
      }
    }

    yield [...state.values()];
  }
}
```

### `consumeFeed`

```ts
function consumeFeed<T>(
  feed: RFeed<T>,
  connection?: Connection,
  signal?: AbortSignal,
): AsyncIterable<T>;
```

A simple wrapper around RethinkDB `.changes()` that turns it it into an Async
Iterable.

#### Examples <!-- omit in toc -->

Async generator that emits live point query states:

```ts
import {r} from 'rethinkdb-ts';
import {consumeFeed} from 'rethinkdb-watch';
import {AbortSignal} from 'node-abort-controller';

const connection = await r.connect({host, port});

const query = r.db('test').table<Doc>('test').get('test-id');

const feed = query.changes({
  squash: true,
  includeInitial: true,
});

async function* watchDocState(signal: AbortSignal): AsyncGenerator<Doc | null> {
  for await (const changes of consumeFeed(feed, connection, signal)) {
    if (changes.new_val != null) {
      yield changes.new_val;
    } else {
      yield null;
    }
  }
}
```

[npm-image]: https://badge.fury.io/js/rethinkdb-watch.svg
[npm-url]: https://badge.fury.io/js/rethinkdb-watch
