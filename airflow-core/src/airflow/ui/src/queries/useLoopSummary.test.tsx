/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useDagRunServiceGetDagRun, useGridServiceGetLoopSummary } from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import { useLoopSummary } from "./useLoopSummary";

vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRun: vi.fn(),
  useGridServiceGetLoopSummary: vi.fn(() => ({ data: { status: "failed" } })),
}));
vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  useAutoRefresh: () => 3000,
}));

describe("useLoopSummary", () => {
  it.each([
    ["running", 3000],
    ["success", false],
  ] as const)("uses DagRun %s liveness even when the loop has a failed body task", (state, interval) => {
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({ data: { state } } as ReturnType<
      typeof useDagRunServiceGetDagRun
    >);
    renderHook(() => useLoopSummary({ dagId: "dag", groupId: "loop", runId: "run" }), { wrapper: Wrapper });
    expect(useGridServiceGetLoopSummary).toHaveBeenLastCalledWith(
      expect.objectContaining({ dagId: "dag", groupId: "loop", runId: "run" }),
      undefined,
      expect.objectContaining({ enabled: true, refetchInterval: interval, retry: false }),
    );
  });
});
