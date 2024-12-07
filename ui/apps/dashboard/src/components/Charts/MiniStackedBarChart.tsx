'use client';

import { Bar, BarChart, ResponsiveContainer } from 'recharts';

import cn from '@/utils/cn';

type MiniStackedBarChartProps = {
  data: {
    startCount: number;
    failureCount?: number;
  }[];
  className?: string;
};

export default function MiniStackedBarChart({ data, className = '' }: MiniStackedBarChartProps) {
  // Recharts doesn't support stacked bar charts with negative values, so we need to map the data
  // to a new format that works with the chart.
  const mappedData = data.map((d) => ({
    nonFailureCount: d.startCount - (d.failureCount ?? 0),
    failureCount: d.failureCount ?? 0,
  }));

  return (
    <div
      className={cn('border-subtle bg-canvasBase box-border h-8 w-40 rounded border', className)}
    >
      <ResponsiveContainer>
        <BarChart
          data={mappedData}
          barCategoryGap={2}
          margin={{ top: 4, right: 4, bottom: 4, left: 4 }}
        >
          <Bar
            dataKey="failureCount"
            stackId="slot"
            fill="rgb(var(--color-background-error))"
            radius={1}
            barSize={4}
          />
          <Bar
            dataKey="nonFailureCount"
            stackId="slot"
            fill="rgb(var(--color-carbon-300))"
            minPointSize={1}
            barSize={4}
            radius={[1, 1, 0, 0]}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
