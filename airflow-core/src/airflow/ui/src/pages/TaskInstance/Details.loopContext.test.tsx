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
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { TaskInstanceHistoryResponse } from "openapi/requests/types.gen";

import { useTaskInstanceView } from "src/hooks/useTaskInstanceView";
import { Wrapper } from "src/utils/Wrapper";

import { Details } from "./Details";

vi.mock("src/hooks/useTaskInstanceView", () => ({ useTaskInstanceView: vi.fn() }));
vi.mock("src/components/TaskTrySelect", () => ({ TaskTrySelect: () => null }));
vi.mock("src/components/DagVersionDetails", () => ({ DagVersionDetails: () => null }));
vi.mock("./ExtraLinks", () => ({ ExtraLinks: () => null }));
vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useTaskInstanceServiceGetTaskInstanceTryDetails: () => ({ data: undefined }),
}));

describe("Details loop context", () => {
  it("shows retained nested iteration context separately from the mapped slot", () => {
    const task = {
      id: "retained-task",
      loop_iterations: [
        { iteration: 2, loop_id: "outer" },
        { iteration: 4, loop_id: "outer.inner" },
      ],
      map_index: 8,
      state: "success",
      try_number: 1,
    } as TaskInstanceHistoryResponse;

    vi.mocked(useTaskInstanceView).mockReturnValue({
      historical: true,
      historicalTaskInstance: task,
      taskInstance: task,
    } as ReturnType<typeof useTaskInstanceView>);
    render(<Details />, { wrapper: Wrapper });
    const outer = screen.getByRole("row", { name: "taskInstance.iteration (outer) 2" });
    const inner = screen.getByRole("row", { name: "taskInstance.iteration (outer.inner) 4" });

    expect(within(outer).getByText("2")).toBeInTheDocument();
    expect(within(inner).getByText("4")).toBeInTheDocument();
    expect(screen.getByRole("row", { name: "mapIndex 8" })).toBeInTheDocument();
  });
});
