import { MetricsIcon } from '@inngest/components/icons/sections/Metrics';
import { RunsIcon } from '@inngest/components/icons/sections/Runs';

import type { Environment as EnvType } from '@/utils/environments';
import { MenuItem } from './MenuItem';
import { getNavRoute } from './Navigation';

export default function Monitor({
  activeEnv,
  collapsed,
}: {
  activeEnv: EnvType;
  collapsed: boolean;
}) {
  return (
    <div className={`jusity-center flex flex-col ${collapsed ? 'mt-2' : 'mt-5'}`}>
      {collapsed ? (
        <hr className="bg-subtle mx-auto mb-1 w-6" />
      ) : (
        <div className="text-disabled leading-4.5 mx-2.5 mb-1 text-xs font-medium">Monitor</div>
      )}
      <MenuItem
        href={getNavRoute(activeEnv, 'metrics')}
        collapsed={collapsed}
        text="Metrics"
        icon={<MetricsIcon className="h-18px w-[18px]" />}
        comingSoon={true}
      />
      <MenuItem
        href={getNavRoute(activeEnv, 'functions/monitor')}
        collapsed={collapsed}
        text="Runs"
        icon={<RunsIcon className="h-18px w-[18px]" />}
        comingSoon={true}
      />
      {/* <MenuItem
        href={getNavRoute(activeEnv, 'events/monitor')}
        collapsed={collapsed}
        text="Event Logs"
        icon={<EventLogsIcon className="h-18px w-[18px]" />}
        comingSoon={true}
      /> */}
    </div>
  );
}