import type { ReactNode } from 'react';
import { Tooltip, TooltipContent, TooltipTrigger } from '@inngest/components/Tooltip/Tooltip';

export const OptionalTooltip = ({
  children,
  tooltip,
}: {
  children: ReactNode;
  tooltip?: ReactNode;
}) =>
  tooltip ? (
    <Tooltip>
      <TooltipTrigger asChild>{children}</TooltipTrigger>
      <TooltipContent
        side="right"
        className="text-subtle flex h-8 items-center px-4 text-xs leading-[18px]"
      >
        {tooltip}
      </TooltipContent>
    </Tooltip>
  ) : (
    children
  );