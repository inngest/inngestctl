'use client';

import { useEffect, useState } from 'react';

import {
  STATUS_PAGE_URL,
  getStatus,
  indicatorColor,
  type Status,
} from '@/components/Support/Status';

export function useSystemStatus() {
  const [status, setStatus] = useState<Status>({
    url: STATUS_PAGE_URL,
    description: 'Fetching status...',
    impact: 'none',
    indicatorColor: indicatorColor.none,
    updated_at: '',
  });
  useEffect(() => {
    (async function () {
      setStatus(await getStatus());
    })();
  }, []);
  return status;
}
