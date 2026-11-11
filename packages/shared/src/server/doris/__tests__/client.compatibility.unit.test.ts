import { EventEmitter } from "events";
import { Readable } from "stream";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ createPool: vi.fn(), error: vi.fn() }));
vi.mock("mysql2/promise", () => ({
  default: { createPool: mocks.createPool },
}));
vi.mock("axios", () => ({
  default: {
    create: vi.fn(() => ({
      interceptors: { request: { use: vi.fn() }, response: { use: vi.fn() } },
    })),
  },
}));
vi.mock("../../../env", () => ({
  env: {
    DORIS_FE_HTTP_URL: "http://localhost:8030",
    DORIS_FE_QUERY_PORT: 9030,
    DORIS_DB: "test",
    DORIS_USER: "test",
    DORIS_PASSWORD: "test",
    DORIS_REQUEST_TIMEOUT_MS: 30000,
    DORIS_MAX_OPEN_CONNECTIONS: 10,
  },
}));
vi.mock("../../instrumentation", () => ({ getCurrentSpan: vi.fn() }));
vi.mock("@opentelemetry/api", () => ({
  propagation: { inject: vi.fn() },
  context: { active: vi.fn() },
}));
vi.mock("../../logger", () => ({
  logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: mocks.error },
}));

import { DorisClient } from "../client";

describe("Doris 3.1 client compatibility", () => {
  let pool: EventEmitter & {
    query: ReturnType<typeof vi.fn>;
    getConnection: ReturnType<typeof vi.fn>;
  };
  let client: DorisClient;

  beforeEach(() => {
    vi.clearAllMocks();
    pool = Object.assign(new EventEmitter(), {
      query: vi.fn(),
      getConnection: vi.fn(),
    });
    mocks.createPool.mockReturnValue(pool);
    client = new DorisClient();
  });

  it("unions tag arrays without expanding rows or rewriting filter literals", async () => {
    const rows = [{ tags_union: '["codex","mac"]', total_tokens: 10 }];
    pool.query.mockResolvedValue([rows]);
    const result = await client.query(
      "SELECT group_array_union(t.tags) AS tags_union, SUM(tokens) AS total_tokens FROM t WHERE name = 'group_array_union(t.tags)' /* group_array_union(t.tags) */",
    );
    expect(pool.query).toHaveBeenCalledWith(
      "SELECT array_distinct(array_flatten(collect_list(t.tags))) AS tags_union, SUM(tokens) AS total_tokens FROM t WHERE name = 'group_array_union(t.tags)' /* group_array_union(t.tags) */",
    );
    expect(result).toEqual(rows);
  });

  it("handles tag unions nested inside another SQL function on streamed queries", async () => {
    const query = vi.fn(() => ({
      stream: () => Readable.from([{ trace_tags: '["codex"]' }]),
    }));
    const connection = { connection: { query }, release: vi.fn() };
    pool.getConnection.mockResolvedValue(connection);
    const rows = [];
    for await (const row of client.queryStream(
      "SELECT array_remove(GROUP_ARRAY_UNION(o.tags), '') AS trace_tags FROM t o GROUP BY session_id",
    ))
      rows.push(row);
    expect(query).toHaveBeenCalledWith(
      "SELECT array_remove(array_distinct(array_flatten(collect_list(o.tags))), '') AS trace_tags FROM t o GROUP BY session_id",
    );
    expect(rows).toEqual([{ trace_tags: '["codex"]' }]);
    expect(connection.release).toHaveBeenCalledOnce();
  });

  it("reads nested metadata without the unsupported SQL function and preserves literals", async () => {
    const metadata =
      '{"a":{"b":2,"empty":{}},"items":[{"x":1}],"n":null,"__proto__":{}}';
    pool.query.mockResolvedValue([[{ id: "trace", metadata }]]);
    const rows = await client.query(
      "SELECT json_object_flatten(t.metadata) AS metadata FROM t WHERE input = 'json_object_flatten(metadata)' /* json_object_flatten(metadata) */",
    );
    expect(pool.query).toHaveBeenCalledWith(
      "SELECT CAST(t.metadata AS STRING) AS metadata FROM t WHERE input = 'json_object_flatten(metadata)' /* json_object_flatten(metadata) */",
    );
    expect(JSON.parse(rows[0].metadata)).toEqual(
      JSON.parse(
        '{"a.b":2,"a.empty":{},"items":[{"x":1}],"n":null,"__proto__":{}}',
      ),
    );
  });

  it("normalizes metadata on streamed query rows and releases the connection", async () => {
    const query = vi.fn(() => ({
      stream: () => Readable.from([{ metadata: '{"tool":{"status":"ok"}}' }]),
    }));
    const connection = { connection: { query }, release: vi.fn() };
    pool.getConnection.mockResolvedValue(connection);
    const rows = [];
    for await (const row of client.queryStream(
      "SELECT json_object_flatten(metadata) AS metadata FROM t",
    ))
      rows.push(row);
    expect(query).toHaveBeenCalledWith(
      "SELECT CAST(metadata AS STRING) AS metadata FROM t",
    );
    expect(JSON.parse(rows[0].metadata)).toEqual({ "tool.status": "ok" });
    expect(connection.release).toHaveBeenCalledOnce();
  });

  it("initializes each new connection for partitioned telemetry deletes", () => {
    const first = { query: vi.fn(), destroy: vi.fn() };
    const second = { query: vi.fn(), destroy: vi.fn() };
    pool.emit("connection", first);
    pool.emit("connection", second);
    for (const connection of [first, second]) {
      expect(connection.query).toHaveBeenCalledWith(
        "SET SESSION delete_without_partition = true",
        expect.any(Function),
      );
      expect(connection.destroy).not.toHaveBeenCalled();
    }
  });

  it("discards a connection if its session cannot be initialized", () => {
    const failure = new Error("session initialization failed");
    const connection = {
      query: vi.fn((_sql: string, callback: (error: Error) => void) =>
        callback(failure),
      ),
      destroy: vi.fn(),
    };
    pool.emit("connection", connection);
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(mocks.error).toHaveBeenCalledWith(
      "Failed to initialize Doris connection session",
      { error: failure.message },
    );
  });
});
