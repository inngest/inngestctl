'use client';

import { useMemo } from 'react';
import { type Route } from 'next';
import { useRouter } from 'next/navigation';
import { NewButton } from '@inngest/components/Button';
import { Time } from '@inngest/components/Time';
import { useQuery } from 'urql';

import { useEnvironment } from '@/components/Environments/environment-context';
import { graphql } from '@/gql';
import { pathCreator } from '@/utils/urls';

const GetFailedFunctionRunsDocument = graphql(`
  query GetFailedFunctionRuns(
    $environmentID: ID!
    $functionSlug: String!
    $lowerTime: Time!
    $upperTime: Time!
  ) {
    environment: workspace(id: $environmentID) {
      function: workflowBySlug(slug: $functionSlug) {
        failedRuns: runsV2(
          filter: {
            lowerTime: $lowerTime
            status: [FAILED]
            timeField: ENDED_AT
            upperTime: $upperTime
          }
          first: 20
        ) {
          edges {
            node {
              id
              endedAt
            }
          }
        }
      }
    }
  }
`);

type LatestFailedFunctionRunsProps = {
  environmentSlug: string;
  functionSlug: string;
};

const second = 1_000;
const minute = 60 * second;
const hour = 60 * minute;
const day = 24 * hour;

export default function LatestFailedFunctionRuns({
  environmentSlug,
  functionSlug,
}: LatestFailedFunctionRunsProps) {
  const lowerTime = useMemo(() => {
    return new Date(new Date().valueOf() - 14 * day);
  }, []);

  const upperTime = useMemo(() => {
    return new Date();
  }, []);

  const environment = useEnvironment();
  const [{ data: failedFunctionRunsResponse, fetching: isFetchingFailedFunctionRuns }] = useQuery({
    query: GetFailedFunctionRunsDocument,
    variables: {
      environmentID: environment.id,
      functionSlug,
      lowerTime: lowerTime.toISOString(),
      upperTime: upperTime.toISOString(),
    },
  });
  const router = useRouter();

  const failedFunctionRuns =
    failedFunctionRunsResponse?.environment.function?.failedRuns?.edges?.map((edge) => edge?.node);

  return (
    <div>
      <header className="flex items-center justify-between gap-3 py-3">
        <h1 className="text-basis font-semibold">Latest Failed Runs</h1>
        <NewButton
          kind="secondary"
          className="ml-auto"
          appearance="outlined"
          href={
            `/env/${environmentSlug}/functions/${encodeURIComponent(functionSlug)}/runs` as Route
          }
          label="View All Runs"
        />
      </header>
      <div className="border-subtle text-subtle rounded-md border text-sm">
        <table className="divide-subtle bg-surfaceBase w-full table-fixed divide-y rounded-lg">
          <thead className="h-full text-left">
            <tr>
              <th className="p-4 font-semibold" scope="col">
                Occurred at
              </th>
              <th className="font-semibold" scope="col">
                ID
              </th>
            </tr>
          </thead>
          <tbody className="divide-subtle h-full divide-y">
            {!failedFunctionRuns && isFetchingFailedFunctionRuns && (
              <tr>
                <td className="p-4 text-center" colSpan={3}>
                  Loading...
                </td>
              </tr>
            )}
            {failedFunctionRuns && failedFunctionRuns.length > 0
              ? failedFunctionRuns.map((functionRun, index) => {
                  if (!functionRun) {
                    return (
                      <tr key={index} className="opacity-50">
                        <td colSpan={2} className="p-4 text-center font-semibold text-slate-700">
                          Error: could not load function run
                        </td>
                      </tr>
                    );
                  }

                  // Should always be false since failed functions are
                  // inherently ended, but TypeScript can't know that.
                  if (!functionRun.endedAt) {
                    return null;
                  }

                  return (
                    <tr
                      key={functionRun.id}
                      className="hover:bg-surfaceSubtle cursor-pointer truncate transition-all"
                      onClick={() =>
                        router.push(
                          pathCreator.runPopout({ envSlug: environmentSlug, runID: functionRun.id })
                        )
                      }
                    >
                      <td className="p-4">
                        <Time
                          className="text-basis inline-flex min-w-[112px] pr-6 font-semibold"
                          format="relative"
                          value={new Date(functionRun.endedAt)}
                        />

                        <Time value={new Date(functionRun.endedAt)} />
                      </td>
                      <td className="font-mono text-xs font-medium">{functionRun.id}</td>
                    </tr>
                  );
                })
              : failedFunctionRuns && (
                  <tr>
                    <td className="p-4 text-center" colSpan={3}>
                      No failures.
                    </td>
                  </tr>
                )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
