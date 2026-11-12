import { EventEmitter } from "events";
import http from "http";
import type { AddressInfo } from "net";
import { Readable } from "stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  createPool: vi.fn(),
  env: {
    DORIS_FE_HTTP_URL: "http://127.0.0.1:1",
    DORIS_FE_HTTP_URLS: undefined as string | undefined,
    DORIS_FE_QUERY_PORT: 9030,
    DORIS_DB: "test",
    DORIS_USER: "test",
    DORIS_PASSWORD: "secret",
    DORIS_REQUEST_TIMEOUT_MS: 300,
    DORIS_FE_CONNECT_TIMEOUT_MS: 100,
    DORIS_MAX_OPEN_CONNECTIONS: 4,
  },
}));
vi.mock("mysql2/promise", () => ({
  default: { createPool: mocks.createPool },
}));
vi.mock("../../../env", () => ({ env: mocks.env }));
vi.mock("../../instrumentation", () => ({ getCurrentSpan: vi.fn() }));
vi.mock("@opentelemetry/api", () => ({
  propagation: { inject: vi.fn() },
  context: { active: vi.fn() },
}));
vi.mock("../../logger", () => ({
  logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { DorisClient, DorisClientManager } from "../client";

const failure = (code = "ECONNREFUSED") =>
  Object.assign(new Error(code), { code });
const makePool = () =>
  Object.assign(new EventEmitter(), {
    query: vi.fn().mockRejectedValue(failure()),
    getConnection: vi.fn(),
    end: vi.fn().mockResolvedValue(undefined),
  });
const connection = () => ({
  query: vi.fn().mockResolvedValue([[{ value: 1 }]]),
  release: vi.fn(),
  destroy: vi.fn(),
});

describe("Doris client native FE failover", () => {
  let pools: ReturnType<typeof makePool>[];
  let clients: DorisClient[];
  let servers: http.Server[];

  beforeEach(() => {
    vi.clearAllMocks();
    mocks.env.DORIS_FE_HTTP_URLS = undefined;
    pools = [makePool(), makePool(), makePool()];
    let created = 0;
    mocks.createPool.mockImplementation(() => pools[created++] ?? makePool());
    clients = [];
    servers = [];
  });

  afterEach(async () => {
    await Promise.all(clients.map((client) => client.close()));
    await Promise.all(
      servers.map((server) => {
        server.closeAllConnections();
        return new Promise<void>((resolve) => server.close(() => resolve()));
      }),
    );
    vi.restoreAllMocks();
  });

  function client(urls = ["http://fe-a:8030", "http://fe-b:8030"]) {
    const result = new DorisClient({
      feHttpUrls: urls,
      timeout: 300,
      feConnectTimeoutMs: 100,
    });
    clients.push(result);
    return result;
  }

  async function serve(handler: http.RequestListener) {
    const server = http.createServer(handler);
    servers.push(server);
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  }

  it("acquires a connection from another FE when the first FE cannot connect", async () => {
    const healthy = connection();
    pools[0].getConnection.mockRejectedValue(failure());
    pools[1].getConnection.mockResolvedValue(healthy);
    expect(await client().query("SELECT 1 AS value")).toEqual([{ value: 1 }]);
    expect(healthy.release).toHaveBeenCalledOnce();
    expect(mocks.createPool.mock.calls.map(([config]) => config.host)).toEqual([
      "fe-a",
      "fe-b",
    ]);
  });

  it("can fail over before dispatching a mutating statement", async () => {
    const healthy = connection();
    healthy.query.mockResolvedValue([{}]);
    pools[0].getConnection.mockRejectedValue(failure());
    pools[1].getConnection.mockResolvedValue(healthy);
    await client().query("DELETE FROM traces WHERE id = 'test'");
    expect(healthy.query).toHaveBeenCalledOnce();
  });

  it("retries a read when an established connection is lost", async () => {
    const broken = connection();
    broken.query.mockRejectedValue(failure("PROTOCOL_CONNECTION_LOST"));
    pools[0].getConnection.mockResolvedValue(broken);
    pools[1].getConnection.mockResolvedValue(connection());
    expect(await client().query("SELECT 1 AS value")).toEqual([{ value: 1 }]);
    expect(broken.destroy).toHaveBeenCalledOnce();
  });

  it("does not replay a mutation after dispatch when its outcome is unknown", async () => {
    const broken = connection();
    broken.query.mockRejectedValue(failure("ECONNRESET"));
    pools[0].getConnection.mockResolvedValue(broken);
    pools[1].getConnection.mockResolvedValue(connection());
    await expect(
      client().query("INSERT INTO events VALUES (1)"),
    ).rejects.toMatchObject({ code: "ECONNRESET" });
    expect(pools[1].getConnection).not.toHaveBeenCalled();
  });

  it("does not hide authentication errors by trying other FEs", async () => {
    pools[0].getConnection.mockRejectedValue(failure("ER_ACCESS_DENIED_ERROR"));
    await expect(client().query("SELECT 1")).rejects.toMatchObject({
      code: "ER_ACCESS_DENIED_ERROR",
    });
    expect(pools[1].getConnection).not.toHaveBeenCalled();
  });

  it.each([
    "SELECT 1; INSERT INTO events VALUES (1)",
    "SELECT value FROM t INTO OUTFILE 's3://bucket/file'",
  ])("does not replay a potentially mutating SELECT: %s", async (sql) => {
    const broken = connection();
    broken.query.mockRejectedValue(failure("ECONNRESET"));
    pools[0].getConnection.mockResolvedValue(broken);
    pools[1].getConnection.mockResolvedValue(connection());
    await expect(client().query(sql)).rejects.toMatchObject({
      code: "ECONNRESET",
    });
    expect(pools[1].getConnection).not.toHaveBeenCalled();
  });

  it("backs off an unavailable FE and tries it again after recovery", async () => {
    const now = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(now);
    pools[0].getConnection.mockRejectedValue(failure());
    pools[1].getConnection.mockResolvedValue(connection());
    const instance = client();
    await instance.query("SELECT 1");
    await instance.query("SELECT 1");
    expect(pools[0].getConnection).toHaveBeenCalledOnce();
    pools[0].getConnection.mockResolvedValue(connection());
    clock.mockReturnValue(now + 11_000);
    await instance.query("SELECT 1");
    expect(pools[0].getConnection).toHaveBeenCalledTimes(2);
  });

  it("retries a streamed read that fails before its first row", async () => {
    const broken = {
      connection: {
        query: vi.fn(() => ({
          stream: () =>
            new Readable({
              objectMode: true,
              read() {
                this.destroy(failure("ECONNRESET"));
              },
            }),
        })),
      },
      release: vi.fn(),
      destroy: vi.fn(),
    };
    const healthy = {
      connection: {
        query: vi.fn(() => ({ stream: () => Readable.from([{ value: 1 }]) })),
      },
      release: vi.fn(),
      destroy: vi.fn(),
    };
    pools[0].getConnection.mockResolvedValue(broken);
    pools[1].getConnection.mockResolvedValue(healthy);
    const rows = [];
    for await (const row of client().queryStream("SELECT value FROM t"))
      rows.push(row);
    expect(rows).toEqual([{ value: 1 }]);
    expect(broken.destroy).toHaveBeenCalledOnce();
    expect(healthy.release).toHaveBeenCalledOnce();
  });

  it("does not replay a partially consumed stream", async () => {
    const broken = {
      connection: {
        query: vi.fn(() => ({
          stream: () =>
            Readable.from(
              (async function* () {
                yield { value: 1 };
                throw failure("ECONNRESET");
              })(),
            ),
        })),
      },
      release: vi.fn(),
      destroy: vi.fn(),
    };
    pools[0].getConnection.mockResolvedValue(broken);
    const rows: unknown[] = [];
    const consume = async () => {
      for await (const row of client().queryStream("SELECT value FROM t"))
        rows.push(row);
    };
    await expect(consume()).rejects.toMatchObject({ code: "ECONNRESET" });
    expect(rows).toEqual([{ value: 1 }]);
    expect(pools[1].getConnection).not.toHaveBeenCalled();
  });

  it("initializes sessions and closes every FE pool", async () => {
    const instance = client();
    for (const pool of pools.slice(0, 2)) {
      const raw = { query: vi.fn(), destroy: vi.fn() };
      pool.emit("connection", raw);
      expect(raw.query).toHaveBeenCalledWith(
        "SET SESSION delete_without_partition = true",
        expect.any(Function),
      );
    }
    await instance.close();
    expect(pools[0].end).toHaveBeenCalledOnce();
    expect(pools[1].end).toHaveBeenCalledOnce();
  });

  it("uses the environment FE list and lets an explicit single URL override it", async () => {
    mocks.env.DORIS_FE_HTTP_URLS = "http://fe-a:8030, http://fe-b:8030";
    const inherited = new DorisClient();
    clients.push(inherited);
    expect(mocks.createPool).toHaveBeenCalledTimes(2);
    const explicit = new DorisClient({ feHttpUrl: "http://fe-c:8030" });
    clients.push(explicit);
    expect(mocks.createPool.mock.calls[2][0].host).toBe("fe-c");
  });

  it("keeps different FE lists in separate singleton clients", async () => {
    const manager = DorisClientManager.getInstance();
    const first = manager.getClient({
      feHttpUrls: ["http://fe-a:8030", "http://fe-b:8030"],
    });
    // Reuse the fake pools if another instance needs more than three pools.
    mocks.createPool.mockImplementation(() => makePool());
    const second = manager.getClient({
      feHttpUrls: ["http://fe-a:8030", "http://fe-c:8030"],
    });
    expect(second).not.toBe(first);
    await manager.closeAllConnections();
  });

  it("fails over the empty FE probe without replaying the BE body", async () => {
    const bodies: string[] = [];
    const labels: (string | undefined)[] = [];
    const be = await serve((req, res) => {
      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        bodies.push(body);
        labels.push(req.headers.label as string);
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ Status: "Success", NumberLoadedRows: 1 }));
      });
    });
    const offline = await serve((req) => req.socket.destroy());
    const fe = await serve((_req, res) => {
      res.writeHead(307, { Location: be + "/load" });
      res.end();
    });
    await client([offline, fe]).streamLoad("traces", [{ id: "one" }], {
      label: "stable-label",
    });
    expect(bodies).toEqual(['[{"id":"one"}]']);
    expect(labels).toEqual(["stable-label"]);
  });

  it("does not switch FE and replay a BE upload after a broken BE response", async () => {
    let uploads = 0;
    const be = await serve((req) => {
      uploads++;
      req.resume();
      req.on("end", () => req.socket.destroy());
    });
    const first = await serve((_req, res) => {
      res.writeHead(307, { Location: be + "/load" });
      res.end();
    });
    let secondCalls = 0;
    const second = await serve((_req, res) => {
      secondCalls++;
      res.writeHead(307, { Location: be + "/load" });
      res.end();
    });
    await expect(
      client([first, second]).streamLoad("traces", [{ id: "one" }], {
        label: "stable-label",
      }),
    ).rejects.toThrow();
    expect(uploads).toBe(1);
    expect(secondCalls).toBe(0);
  });

  it("bounds an unresponsive FE probe and switches to another FE", async () => {
    const unresponsive = await serve(() => {});
    const healthy = await serve((_req, res) => {
      res.setHeader("Content-Type", "application/json");
      res.end('{"code":0}');
    });
    expect(await client([unresponsive, healthy]).healthCheck()).toBe(true);
  });

  it("fails over HTTP 503 but preserves an HTTP authentication failure", async () => {
    const unavailable = await serve((_req, res) => {
      res.writeHead(503);
      res.end();
    });
    let healthyCalls = 0;
    const healthy = await serve((_req, res) => {
      healthyCalls++;
      res.end("{}");
    });
    expect(await client([unavailable, healthy]).healthCheck()).toBe(true);
    expect(healthyCalls).toBe(1);
    const denied = await serve((_req, res) => {
      res.writeHead(401);
      res.end();
    });
    expect(await client([denied, healthy]).healthCheck()).toBe(false);
    expect(healthyCalls).toBe(1);
  });
});
