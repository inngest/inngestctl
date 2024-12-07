import { Fragment } from 'react';
import { Listbox, Transition } from '@headlessui/react';
import { RiArrowDownSLine } from '@remixicon/react';
import dayjs from 'dayjs';
import { useQuery } from 'urql';

import {
  getMinimumPlanForLogRetention,
  transformPlans,
  type TimeRange,
} from '@/app/(organization-active)/(dashboard)/env/[environmentSlug]/functions/[slug]/logs/TimeRangeFilter';
import { graphql } from '@/gql';

const currentTime = new Date();

const timeRanges: TimeRange[] = [
  {
    key: '30m',
    start: new Date(currentTime.valueOf() - 30 * 60 * 1_000), // 30 Minutes
    end: currentTime,
  },
  {
    key: '60m',
    start: new Date(currentTime.valueOf() - 60 * 60 * 1_000), // 60 Minutes
    end: currentTime,
  },
  {
    key: '6h',
    start: new Date(currentTime.valueOf() - 6 * 60 * 60 * 1_000), // 6 Hours
    end: currentTime,
  },
  {
    key: '12h',
    start: new Date(currentTime.valueOf() - 12 * 60 * 60 * 1_000), // 12 Hours
    end: currentTime,
  },
  {
    key: '24h',
    start: new Date(currentTime.valueOf() - 24 * 60 * 60 * 1_000), // 24 Hours
    end: currentTime,
  },
  {
    key: '3d',
    start: new Date(currentTime.valueOf() - 3 * 24 * 60 * 60 * 1_000), // 3 Days
    end: currentTime,
  },
  {
    key: '7d',
    start: new Date(currentTime.valueOf() - 7 * 24 * 60 * 60 * 1_000), // 7 Days
    end: currentTime,
  },
  {
    key: '14d',
    start: new Date(currentTime.valueOf() - 14 * 24 * 60 * 60 * 1_000), // 14 Days
    end: currentTime,
  },
  {
    key: '30d',
    start: new Date(currentTime.valueOf() - 30 * 24 * 60 * 60 * 1_000), // 30 Days
    end: currentTime,
  },
];

export const defaultTimeRange = timeRanges[4]!;

export function getTimeRangeByKey(key: string): TimeRange | undefined {
  return timeRanges.find((timeRange) => timeRange.key === key);
}

const GetBillingPlanDocument = graphql(`
  query GetBillingPlan {
    account {
      plan {
        id
        name
        features
      }
    }

    plans {
      name
      features
    }
  }
`);

type DashboardTimeRangeFilterProps = {
  selectedTimeRange: TimeRange;
  onTimeRangeChange: (timeRange: TimeRange) => void;
};

// TODO: merge this with TimeRangeFilter.tsx
export default function DashboardTimeRangeFilter({
  selectedTimeRange,
  onTimeRangeChange,
}: DashboardTimeRangeFilterProps) {
  const [{ data }] = useQuery({
    query: GetBillingPlanDocument,
  });

  // Since "features" is a map, we can't be 100% sure that there's a log
  // retention value. So default to 7 days.
  let logRetention = 7;
  if (typeof data?.account.plan?.features.log_retention === 'number') {
    logRetention = data.account.plan.features.log_retention;
  }

  let plans: ReturnType<typeof transformPlans> | undefined;
  if (data?.plans) {
    plans = transformPlans(data.plans);
  }

  return (
    <Listbox value={selectedTimeRange} onChange={onTimeRangeChange}>
      {({ open }) => (
        <>
          <Listbox.Label className="sr-only">Filter by time</Listbox.Label>
          <div className="relative">
            <Listbox.Button className="border-subtle bg-surfaceBase text-basis hover:bg-surfaceSubtle group inline-flex items-center gap-1 rounded-[6px] border px-3 py-[5px] text-sm font-medium capitalize focus:outline-none">
              <p>Last {getTimeRangeLabel(selectedTimeRange)}</p>
              <RiArrowDownSLine className="h-4 w-4" aria-hidden="true" />
            </Listbox.Button>

            <Transition
              show={open}
              as={Fragment}
              leave="transition ease-in duration-100"
              leaveFrom="opacity-100"
              leaveTo="opacity-0"
            >
              <Listbox.Options className="shadow-floating bg-surfaceBase border-subtle absolute right-0 z-10 mt-[5px] w-52 origin-top-right overflow-hidden rounded-md border py-[9px] ring-1 ring-black/5 backdrop-blur-[3px] focus:outline-none">
                {timeRanges.map((timeRange) => {
                  const timeRangeStartInDaysAgo = dayjs(currentTime).diff(
                    dayjs(timeRange.start),
                    'days'
                  );

                  const isPlanSufficient = timeRangeStartInDaysAgo <= logRetention;
                  let minimumPlanName: string | undefined = undefined;
                  if (plans) {
                    minimumPlanName = getMinimumPlanForLogRetention(plans, timeRangeStartInDaysAgo);
                  }

                  return (
                    <Listbox.Option
                      key={timeRange.end.valueOf() - timeRange.start.valueOf()}
                      className="ui-selected:bg-canvasSubtle ui-disabled:text-disabled ui-selected:text-primary-moderate text-basis hover:bg-canvasSubtle/50 flex cursor-pointer select-none items-center justify-between px-3.5 py-1 text-sm font-medium capitalize focus:outline-none"
                      value={timeRange}
                      disabled={!isPlanSufficient}
                    >
                      {getTimeRangeLabel(timeRange)}{' '}
                      {!isPlanSufficient && minimumPlanName && (
                        <span className="text-info ring-subtle inline-flex items-center rounded px-[5px] py-0.5 text-[12px] font-semibold leading-tight ring-1 ring-inset">
                          {minimumPlanName} Plan
                        </span>
                      )}
                    </Listbox.Option>
                  );
                })}
              </Listbox.Options>
            </Transition>
          </div>
        </>
      )}
    </Listbox>
  );
}

export function getTimeRangeLabel(timeRange: TimeRange) {
  return dayjs(timeRange.start).from(timeRange.end, true);
}
