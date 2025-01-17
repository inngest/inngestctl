type Impact = 'partial_outage' | 'degraded_performance' | 'full_outage';
type Indicator = Impact | 'none' | 'maintenance';
type StatusEvent = {
  id: string;
  name: string;
  url: string;
  last_update_at: string; // ISO-8601
  last_update_message: string;
  affected_components: {
    id: string;
    name: string;
    group_name?: string;
  }[];
};
type Incident = StatusEvent & {
  status: 'identified' | 'investigating' | 'monitoring';
  current_worst_impact: Impact;
};
type MaintenanceInProgressEvent = StatusEvent & {
  status: 'maintenance_in_progress';
  started_at: string; // ISO-8601
  scheduled_end_at: string; // ISO-8601
};
type MaintenanceScheduledEvent = StatusEvent & {
  status: 'maintenance_scheduled';
  starts_at: string; // ISO-8601
  ends_at: string; // ISO-8601
};

type StatusPageSummaryResponse = {
  page_title: string;
  page_url: string;
  ongoing_incidents: Incident[];
  in_progress_maintenances: MaintenanceInProgressEvent[];
  scheduled_maintenances: MaintenanceScheduledEvent[];
};

export type Status = {
  url: string;
  description: string;
  impact: Indicator;
  indicatorColor: string;
  updated_at: string;
};

const impactMessage: { [K in Indicator]: string } = {
  none: 'All systems operational',
  degraded_performance: 'Degraded performance',
  partial_outage: 'Partial system outage',
  full_outage: 'Major system outage',
  maintenance: 'Maintenance in progress',
};

export const indicatorColor: { [K in Indicator]: string } = {
  none: 'rgba(var(--color-matcha-500))',
  degraded_performance: 'rgb(var(--color-honey-300))',
  maintenance: 'rgb(var(--color-honey-300))',
  partial_outage: 'rgb(var(--color-honey-500))',
  full_outage: 'rgb(var(--color-ruby-500))',
};

export const STATUS_PAGE_URL = 'https://status.inngest.com';

export const mapStatus = (res: StatusPageSummaryResponse) => {
  // Grab first incident and maintenance item
  const incident = res.ongoing_incidents[0];
  const maintenance = res.in_progress_maintenances[0];
  const impact = incident?.current_worst_impact || (maintenance ? 'maintenance' : 'none');
  return {
    indicatorColor: indicatorColor[impact],
    impact,
    description: impactMessage[impact],
    updated_at: incident?.last_update_at || new Date().toString(),
    url: incident?.url || STATUS_PAGE_URL,
  };
};

export const fetchStatus = async (): Promise<StatusPageSummaryResponse> => {
  return await fetch('https://inngest.statuspage.io/api/v2/status.json').then((r) => r.json());
};

export const getStatus = async (): Promise<Status> => {
  return mapStatus(await fetchStatus());
};
