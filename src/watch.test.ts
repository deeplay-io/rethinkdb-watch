import {delay} from 'abort-controller-x';
import {chunk, range, sortBy} from 'lodash';
import {Connection, RDatum, r} from 'rethinkdb-ts';
import {GenericContainer, StartedTestContainer} from 'testcontainers';
import {watch} from './watch';

let rethinkdbContainer: StartedTestContainer;
let connection: Connection;

const db = r.db('test');
const tableName = 'changes';
const table = db.table<{id: number; test?: any}>(tableName);

beforeAll(async () => {
  rethinkdbContainer = await new GenericContainer('rethinkdb:2.4.0')
    .withExposedPorts(28015)
    .start();

  connection = await r.connect({
    host: rethinkdbContainer.getHost(),
    port: rethinkdbContainer.getMappedPort(28015),
  });
}, 60_000);

afterAll(async () => {
  await connection.close();
  await rethinkdbContainer.stop();
}, 60_000);

beforeEach(async () => {
  await db.tableCreate(tableName).run(connection);
});

afterEach(async () => {
  await db.tableDrop(tableName).run(connection);
});

test('basic', async () => {
  const iterator = watch(table, connection)[Symbol.asyncIterator]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {},
          }
        `);

  await table.insert({id: 1}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "1" => Object {
                "newVal": Object {
                  "id": 1,
                },
                "type": "add",
              },
            },
          }
        `);

  await table.get(1).update({test: 'test'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "1" => Object {
                "newVal": Object {
                  "id": 1,
                  "test": "test",
                },
                "oldVal": Object {
                  "id": 1,
                },
                "type": "change",
              },
            },
          }
        `);

  await table.get(1).delete().run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "1" => Object {
                "oldVal": Object {
                  "id": 1,
                  "test": "test",
                },
                "type": "remove",
              },
            },
          }
        `);

  await iterator.return();
});

test('initial', async () => {
  await table.insert([{id: 1}, {id: 2}]).run(connection);

  const iterator = watch(table, connection)[Symbol.asyncIterator]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                },
                "type": "add",
              },
              "1" => Object {
                "newVal": Object {
                  "id": 1,
                },
                "type": "add",
              },
            },
          }
        `);

  setTimeout(() => {
    table.insert({id: 3}).run(connection);
  }, 100);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                },
                "type": "add",
              },
            },
          }
        `);

  await iterator.return();
});

test('buffering', async () => {
  const iterator = watch(table, connection, {
    bufferTimeMs: 250,
  })[Symbol.asyncIterator]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {},
          }
        `);

  const docs = range(10).map(id => ({id}));

  await table.insert(docs).run(connection);

  const result = await iterator.next();

  expect(result.done).toBe(false);
  expect(
    sortBy([...result.value!.values()], update => update.newVal!.id),
  ).toEqual(
    docs.map(doc => ({
      type: 'add',
      newVal: doc,
    })),
  );

  await iterator.return();
});

test('deduplication', async () => {
  await table.insert({id: 1, test: '0'}).run(connection);
  await table.insert({id: 2, test: '0'}).run(connection);

  const iterator = watch(table, connection, {
    bufferTimeMs: 1000,
  })[Symbol.asyncIterator]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                  "test": "0",
                },
                "type": "add",
              },
              "1" => Object {
                "newVal": Object {
                  "id": 1,
                  "test": "0",
                },
                "type": "add",
              },
            },
          }
        `);

  const nextPromise = iterator.next();

  const {signal} = new AbortController();

  await delay(signal, 100);

  const queries = [
    table.get(1).update({test: '1'}),
    table.get(1).update({test: '2'}),
    table.get(1).delete(),
    table.get(2).delete(),
    table.insert({id: 2, test: '1'}),
    table.insert({id: 3, test: '0'}),
    table.get(3).update({test: '1'}),
    table.get(3).delete(),
    table.insert({id: 3, test: '2'}),
  ];

  for (const query of queries) {
    await query.run(connection);
    await delay(signal, 50);
  }

  await expect(nextPromise).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "1" => Object {
                "oldVal": Object {
                  "id": 1,
                  "test": "0",
                },
                "type": "remove",
              },
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                  "test": "1",
                },
                "oldVal": Object {
                  "id": 2,
                  "test": "0",
                },
                "type": "change",
              },
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                  "test": "2",
                },
                "type": "add",
              },
            },
          }
        `);

  await iterator.return();
});

test('integrity', async () => {
  await table.insert(range(1000).map(i => ({id: i}))).run(connection);

  await Promise.all([
    Promise.resolve().then(async () => {
      for (const ids of chunk(range(1001, 50_001), 1000)) {
        await table.insert(ids.map(id => ({id}))).run(connection);
      }
    }),
    Promise.resolve().then(async () => {
      const receivedIds = new Set<number>();

      for await (const batch of watch(table, connection)) {
        for (const update of batch.values()) {
          expect(update.type).toBe('add');
          expect(receivedIds.has(update.newVal!.id)).toBe(false);
          expect(receivedIds.size).toBeLessThan(50_000);

          receivedIds.add(update.newVal!.id);
        }

        if (receivedIds.size === 50_000) {
          break;
        }
      }

      expect(receivedIds.size).toBe(50_000);
    }),
  ]);
}, 30_000);

test('abort', async () => {
  const abortController = new AbortController();

  const iterator = watch(table, connection, {
    signal: abortController.signal,
  })[Symbol.asyncIterator]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {},
          }
        `);

  setTimeout(() => {
    abortController.abort();
  }, 50);

  await expect(iterator.next()).rejects.toMatchInlineSnapshot(
    `[AbortError: The operation has been aborted]`,
  );
});

test('abort while buffering', async () => {
  const abortController = new AbortController();

  const iterator = watch(table, connection, {
    signal: abortController.signal,
    bufferTimeMs: 60_000,
  })[Symbol.asyncIterator]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
              Object {
                "done": false,
                "value": Map {},
              }
          `);

  const nextPromise = iterator.next();

  await table.insert({id: 1}).run(connection);

  setTimeout(() => {
    abortController.abort();
  }, 50);

  await expect(nextPromise).rejects.toMatchInlineSnapshot(
    `[AbortError: The operation has been aborted]`,
  );
});

test('filter', async () => {
  await table.insert([{id: 1}, {id: 2, test: '0'}, {id: 3}]).run(connection);

  const iterator = watch(table.filter({test: '0'}), connection)[
    Symbol.asyncIterator
  ]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                  "test": "0",
                },
                "type": "add",
              },
            },
          }
        `);

  await table.get(1).update({test: '0'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "1" => Object {
                "newVal": Object {
                  "id": 1,
                  "test": "0",
                },
                "type": "add",
              },
            },
          }
        `);

  await table.get(2).update({test: '1'}).run(connection);
  await table.get(3).update({test: '1'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "2" => Object {
                "oldVal": Object {
                  "id": 2,
                  "test": "0",
                },
                "type": "remove",
              },
            },
          }
        `);

  await iterator.return();
});

test('between', async () => {
  await table.insert([{id: 1}, {id: 2}, {id: 3}]).run(connection);

  const iterator = watch(table.between(2, r.maxval), connection)[
    Symbol.asyncIterator
  ]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                },
                "type": "add",
              },
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                },
                "type": "add",
              },
            },
          }
        `);

  await table.get(2).update({test: '0'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                  "test": "0",
                },
                "oldVal": Object {
                  "id": 2,
                },
                "type": "change",
              },
            },
          }
        `);

  await table.get(3).update({test: '1'}).run(connection);
  await table.get(1).update({test: '1'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                  "test": "1",
                },
                "oldVal": Object {
                  "id": 3,
                },
                "type": "change",
              },
            },
          }
        `);

  await iterator.return();
});

test('getAll', async () => {
  await table.insert([{id: 1}, {id: 2}, {id: 3}]).run(connection);

  const iterator = watch(table.getAll(2, 3), connection)[
    Symbol.asyncIterator
  ]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                },
                "type": "add",
              },
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                },
                "type": "add",
              },
            },
          }
        `);

  await table.get(2).update({test: '0'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                  "test": "0",
                },
                "oldVal": Object {
                  "id": 2,
                },
                "type": "change",
              },
            },
          }
        `);

  await table.get(3).update({test: '1'}).run(connection);
  await table.get(1).update({test: '1'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                  "test": "1",
                },
                "oldVal": Object {
                  "id": 3,
                },
                "type": "change",
              },
            },
          }
        `);

  await iterator.return();
});

test('multiple matches of one document', async () => {
  await table.insert([{id: 1}, {id: 2}, {id: 3}]).run(connection);

  const iterator = watch(table.getAll(2, 2, 3), connection)[
    Symbol.asyncIterator
  ]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                },
                "type": "add",
              },
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                },
                "type": "add",
              },
            },
          }
        `);

  await table.get(2).update({test: '0'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                  "test": "0",
                },
                "oldVal": Object {
                  "id": 2,
                },
                "type": "change",
              },
            },
          }
        `);

  await table.get(3).update({test: '1'}).run(connection);
  await table.get(1).update({test: '1'}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                  "test": "1",
                },
                "oldVal": Object {
                  "id": 3,
                },
                "type": "change",
              },
            },
          }
        `);

  await iterator.return();
});

test('multiple multi-index matches', async () => {
  await table
    .indexCreate('test-index', (row: RDatum) => row('test'), {multi: true})
    .run(connection);

  await table.indexWait('test-index').run(connection);

  await table
    .insert([{id: 1}, {id: 2, test: ['a', 'b']}, {id: 3, test: ['b']}])
    .run(connection);

  const iterator = watch(
    table.getAll('a', 'b', {index: 'test-index'}).filter(row => r.expr(true)),
    connection,
  )[Symbol.asyncIterator]();

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                  "test": Array [
                    "b",
                  ],
                },
                "type": "add",
              },
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                  "test": Array [
                    "a",
                    "b",
                  ],
                },
                "type": "add",
              },
            },
          }
        `);

  await table
    .get(2)
    .update({test: ['a', 'b', 'c']})
    .run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "2" => Object {
                "newVal": Object {
                  "id": 2,
                  "test": Array [
                    "a",
                    "b",
                    "c",
                  ],
                },
                "oldVal": Object {
                  "id": 2,
                  "test": Array [
                    "a",
                    "b",
                  ],
                },
                "type": "change",
              },
            },
          }
        `);

  await table
    .get(3)
    .update({test: ['b', 'c']})
    .run(connection);
  await table
    .get(1)
    .update({test: ['c']})
    .run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
          Object {
            "done": false,
            "value": Map {
              "3" => Object {
                "newVal": Object {
                  "id": 3,
                  "test": Array [
                    "b",
                    "c",
                  ],
                },
                "oldVal": Object {
                  "id": 3,
                  "test": Array [
                    "b",
                  ],
                },
                "type": "change",
              },
            },
          }
        `);

  await table
    .get(2)
    .update({test: ['a']})
    .run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
Object {
  "done": false,
  "value": Map {
    "2" => Object {
      "newVal": Object {
        "id": 2,
        "test": Array [
          "a",
        ],
      },
      "oldVal": Object {
        "id": 2,
        "test": Array [
          "a",
          "b",
          "c",
        ],
      },
      "type": "change",
    },
  },
}
`);

  await table
    .get(2)
    .update({test: ['b']})
    .run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
Object {
  "done": false,
  "value": Map {
    "2" => Object {
      "newVal": Object {
        "id": 2,
        "test": Array [
          "b",
        ],
      },
      "oldVal": Object {
        "id": 2,
        "test": Array [
          "a",
        ],
      },
      "type": "change",
    },
  },
}
`);

  await table
    .get(2)
    .update({test: ['b', 'a']})
    .run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
Object {
  "done": false,
  "value": Map {
    "2" => Object {
      "newVal": Object {
        "id": 2,
        "test": Array [
          "b",
          "a",
        ],
      },
      "oldVal": Object {
        "id": 2,
        "test": Array [
          "b",
        ],
      },
      "type": "change",
    },
  },
}
`);

  await table
    .get(2)
    .update({test: ['a']})
    .run(connection);
  await table.get(2).update({test: []}).run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
Object {
  "done": false,
  "value": Map {
    "2" => Object {
      "oldVal": Object {
        "id": 2,
        "test": Array [
          "b",
          "a",
        ],
      },
      "type": "remove",
    },
  },
}
`);

  await table.get(1).update({test: []}).run(connection);
  await table
    .get(2)
    .update({test: ['a', 'b']})
    .run(connection);

  await expect(iterator.next()).resolves.toMatchInlineSnapshot(`
Object {
  "done": false,
  "value": Map {
    "2" => Object {
      "newVal": Object {
        "id": 2,
        "test": Array [
          "a",
          "b",
        ],
      },
      "type": "add",
    },
  },
}
`);

  await iterator.return();
});
