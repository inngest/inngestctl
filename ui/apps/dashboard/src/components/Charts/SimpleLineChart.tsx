'use client';

import { useMemo } from 'react';
import { Tooltip, TooltipContent, TooltipTrigger } from '@inngest/components/Tooltip';
import { RiErrorWarningLine, RiInformationLine } from '@remixicon/react';
import {
  CartesianGrid,
  Tooltip as ChartTooltip,
  Line,
  LineChart,
  ReferenceArea,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from 'recharts';

import LoadingIcon from '@/icons/LoadingIcon';
import cn from '@/utils/cn';
import { minuteTime } from '@/utils/date';

type SimpleLineChartProps = {
  className?: string;
  height?: number;
  title: string | React.ReactNode;
  desc?: string;
  data?: {
    name: string;
    values: {
      [key: string]: number | boolean;
    };
  }[];
  legend: {
    dataKey: string;
    name: string;
    color: string;
    default?: boolean;
    referenceArea?: boolean;
  }[];
  isLoading: boolean;
  error?: Error;
};

type AxisProps = {
  y: number;
  x: number;
  index: number;
  payload: {
    value: string;
  };
};

function CustomizedXAxisTick(props: AxisProps) {
  return (
    <text
      x={props.x}
      y={props.y}
      dy={16}
      fill="#94A3B8"
      fontSize={10}
      className="font-medium"
      textAnchor="middle"
    >
      {minuteTime(props.payload.value)}
    </text>
  );
}

function CustomizedYAxisTick(props: AxisProps) {
  return (
    <text x={props.x} y={props.y} dy={16} fill="#94A3B8" fontSize={10} className="font-medium">
      {props.index > 0 ? props.payload.value : undefined}
    </text>
  );
}

function omit(obj: Record<string, any>, props: string[]) {
  obj = { ...obj };
  props.forEach((prop) => delete obj[prop]);
  return obj;
}

export default function SimpleLineChart({
  className = '',
  height = 200,
  title,
  desc,
  data = [],
  legend = [],
  isLoading,
  error,
}: SimpleLineChartProps) {
  const referenceAreas = useMemo(() => legend.filter((k) => k.referenceArea), [legend]);
  const referenceAreaKeys = referenceAreas.map((k) => k.dataKey);
  const flattenData = useMemo(() => {
    return data.map((d) => {
      const values = omit(d.values, referenceAreaKeys);
      return { ...values, name: d.name };
    });
  }, [data, referenceAreaKeys]);

  return (
    <div className={cn('border-subtle bg-canvasBase border-b px-6 py-4', className)}>
      <header className="flex items-center justify-between">
        <div className="flex gap-2">
          <h3 className="flex flex-row items-center gap-1.5 font-medium">{title}</h3>
          {desc && (
            <Tooltip>
              <TooltipTrigger>
                <RiInformationLine className="h-4 w-4 text-slate-400" />
              </TooltipTrigger>
              <TooltipContent>{desc}</TooltipContent>
            </Tooltip>
          )}
        </div>
        <div className="flex justify-end gap-4">
          {legend.map((l) => (
            <span key={l.name} className="text-subtle inline-flex items-center text-sm">
              <span
                className="mr-2 inline-flex h-3 w-3 rounded"
                style={{ backgroundColor: l.color }}
              ></span>
              {l.name}
            </span>
          ))}
        </div>
      </header>
      <div style={{ minHeight: `${height}px` }}>
        <ResponsiveContainer height={height} width="100%">
          {isLoading ? (
            <div className="flex h-full w-full items-center justify-center">
              <LoadingIcon />
            </div>
          ) : error ? (
            <div className="flex h-full w-full flex-col items-center justify-center gap-5">
              <div className="inline-flex items-center gap-2 text-red-600">
                <RiErrorWarningLine className="h-4 w-4" />
                <h2 className="text-sm">Failed to load chart</h2>
              </div>
            </div>
          ) : (
            <LineChart data={flattenData} margin={{ top: 16, bottom: 16 }} barCategoryGap={8}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis
                dataKey="name"
                axisLine={false}
                tickLine={false}
                tickSize={2}
                tick={CustomizedXAxisTick}
              />
              <YAxis
                domain={[0, 'auto']}
                allowDataOverflow
                axisLine={false}
                tickLine={false}
                tick={CustomizedYAxisTick}
                width={10}
              />
              <ReferenceArea
                y1={0}
                y2={1}
                x1="2023-10-17T12:30:00Z"
                x2="2023-10-17T12:30:00Z"
                stroke="red"
                fill="red"
              />
              {referenceAreas.map((k) =>
                data.map(({ name, values }, index) => {
                  if (!values[k.dataKey]) return;
                  return (
                    <ReferenceArea
                      key={`${name}+${k.dataKey}`}
                      x1={name}
                      x2={index < data.length - 1 ? data[index + 1]!.name : name}
                      fill={k.color}
                      fillOpacity={0.15}
                    />
                  );
                })
              )}
              <ChartTooltip
                content={(props) => {
                  const { label, payload } = props;
                  return (
                    <div className="rounded-md border bg-white/90 px-3 pb-2 pt-1 text-sm shadow backdrop-blur-md">
                      <span className="text-xs text-slate-500">
                        {new Date(label).toLocaleString()}
                      </span>
                      {payload?.map((p, idx) => {
                        // @ts-ignore
                        if (p.value === false) return;
                        const l = legend.find((l) => l.dataKey == p.name);
                        return (
                          <div
                            key={idx}
                            className="flex items-center text-sm font-medium text-slate-800"
                          >
                            <span
                              className="mr-2 inline-flex h-3 w-3 rounded"
                              style={{ backgroundColor: l?.color || p.color }}
                            ></span>
                            {typeof p.value === 'number'
                              ? p.value.toLocaleString(undefined, {
                                  notation: 'compact',
                                  compactDisplay: 'short',
                                })
                              : ''}{' '}
                            {l?.name || p.name}
                          </div>
                        );
                      }) || ''}
                    </div>
                  );
                }}
                wrapperStyle={{ outline: 'none' }}
                cursor={false}
              />
              {legend.map((l) => (
                <Line
                  dot={false}
                  key={l.name}
                  type="monotone"
                  dataKey={l.dataKey}
                  stroke={l.color}
                />
              ))}
            </LineChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  );
}
