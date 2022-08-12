import {abortable} from 'abort-controller-x';
import {Connection, RFeed} from 'rethinkdb-ts';

/**
 * A simple wrapper around RethinkDB `.changes()` that turns it it into an Async
 * Iterable.
 */
export async function* consumeFeed<T>(
  feed: RFeed<T>,
  connection?: Connection,
  signal: AbortSignal = new AbortController().signal,
): AsyncGenerator<T, void, void> {
  const cursor = await feed.run(connection);

  try {
    yield await abortable(signal, cursor.next());
  } finally {
    await cursor.close();
  }
}
