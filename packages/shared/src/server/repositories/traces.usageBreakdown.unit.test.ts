import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ queryDoris: vi.fn() }));
vi.mock("./doris", () => ({
  queryDoris: mocks.queryDoris,
  commandDoris: vi.fn(),
  queryDorisStream: vi.fn(),
}));
vi.mock("../../db", () => ({ prisma: {} }));
vi.mock("../doris/tableRouting", () => ({ tableFor: () => "project_spans" }));
vi.mock("../doris/crossProjectTableRouting", () => ({
  executeDorisProjectFanout: vi.fn(),
  findFirstDorisProjectTarget: vi.fn(),
}));

import { getTraceUsageBreakdown } from "./traces";

describe("getTraceUsageBreakdown on Doris 3.1", () => {
  beforeEach(() => vi.clearAllMocks());

  it("reads JSON-cast maps and sums native objects and JSON text by key", async () => {
    mocks.queryDoris.mockResolvedValue([
      {
        usage_details: { input: 5, input_cache_read: 10, output: 2, total: 17 },
        cost_details: { input: 0.125, output: 0.25, total: 0.375 },
      },
      {
        usage_details: '{"input":3,"output":1,"total":4}',
        cost_details: '{"input":0.25,"total":0.25}',
      },
      { usage_details: null, cost_details: {} },
    ]);
    const timestamp = new Date("2026-11-11T10:44:45.634Z");
    const result = await getTraceUsageBreakdown({
      projectId: "project_test",
      traceId: "trace_test",
      timestamp,
    });
    const options = mocks.queryDoris.mock.calls[0][0];
    expect(options.query).toContain(
      "CAST(usage_details AS JSON) AS usage_details",
    );
    expect(options.query).toContain(
      "CAST(cost_details AS JSON) AS cost_details",
    );
    expect(options.query).not.toMatch(/\bto_json\s*\(/i);
    expect(options.query).toContain("AND is_root = 0");
    expect(options.query).toContain(
      "DATE_SUB({timestamp: DateTime}, INTERVAL 2 DAY)",
    );
    expect(options.params).toEqual({
      projectId: "project_test",
      traceId: "trace_test",
      timestamp: "2026-11-11 10:44:45.634",
    });
    expect(result).toEqual({
      usageDetails: { input: 8, input_cache_read: 10, output: 3, total: 21 },
      costDetails: { input: 0.375, output: 0.25, total: 0.625 },
    });
  });

  it("returns empty maps when no observations match", async () => {
    mocks.queryDoris.mockResolvedValue([]);
    expect(
      await getTraceUsageBreakdown({
        projectId: "project_test",
        traceId: "missing",
      }),
    ).toEqual({ usageDetails: {}, costDetails: {} });
  });
});
