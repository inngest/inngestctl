'use client';

import { useState } from 'react';
import { Header } from '@inngest/components/Header/Header';

import { ActionsMenu } from '@/components/Events/ActionsMenu';
import ArchiveEventModal from '@/components/Events/ArchiveEventModal';
import SendEventButton from '@/components/Events/SendEventButton';

type EventLayoutProps = {
  children: React.ReactNode;
  params: {
    environmentSlug: string;
    eventName: string;
  };
};

export default function EventLayout({
  children,
  params: { environmentSlug: envSlug, eventName: eventSlug },
}: EventLayoutProps) {
  const eventsPath = `/env/${envSlug}/events`;
  const logsPath = `/env/${envSlug}/events/${eventSlug}/logs`;
  const eventPath = `/env/${envSlug}/events/${eventSlug}`;
  const eventName = decodeURIComponent(eventSlug);
  const [showArchive, setShowArchive] = useState(false);

  return (
    <>
      <Header
        breadcrumb={[
          { text: 'Events', href: eventsPath },
          { text: eventName, href: eventPath },
        ]}
        tabs={[
          {
            href: eventPath,
            children: 'Dashboard',
            exactRouteMatch: true,
          },
          {
            href: logsPath,
            children: 'Logs',
          },
        ]}
        action={
          <div className="flex flex-row items-center justify-end gap-x-1">
            <ActionsMenu archive={() => setShowArchive(true)} />
            <SendEventButton eventName={eventName} newIANav={true} />
          </div>
        }
      />

      <ArchiveEventModal
        eventName={eventName}
        isOpen={showArchive}
        onClose={() => {
          setShowArchive(false);
        }}
      />

      {children}
    </>
  );
}
