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
import { useSearchParams } from "react-router-dom";

import { useDagRunServiceGetDagRun, useGridServiceGetLoopSummary } from "openapi/queries";

import { isStatePending, useAutoRefresh } from "src/utils";

/** Runtime summary of a looped Task Group; polls while the loop is still running. */
export const useLoopSummary = ({
  dagId,
  groupId,
  runId,
}: {
  dagId: string;
  groupId: string;
  runId: string;
}) => {
  const refetchInterval = useAutoRefresh({ dagId });
  const [searchParams] = useSearchParams();
  const { data: dagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId }, undefined, {
    enabled: Boolean(dagId) && Boolean(runId),
    refetchInterval: (query) => isStatePending(query.state.data?.state) && refetchInterval,
  });

  return useGridServiceGetLoopSummary(
    {
      dagId,
      groupId,
      loopRegionId: searchParams.get("loop_region_id") ?? undefined,
      runId,
    },
    undefined,
    {
      enabled: Boolean(dagId) && Boolean(groupId) && Boolean(runId),
      refetchInterval: isStatePending(dagRun?.state) && refetchInterval,
      retry: false,
    },
  );
};
