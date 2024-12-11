'use client';

import { useState } from 'react';
import { Alert } from '@inngest/components/Alert';
import { AlertModal } from '@inngest/components/Modal/AlertModal';
import { useMutation } from 'urql';

import { useEnvironment } from '@/components/Environments/environment-context';
import { graphql } from '@/gql';

const ArchiveEvent = graphql(`
  mutation ArchiveEvent($environmentId: ID!, $name: String!) {
    archiveEvent(workspaceID: $environmentId, name: $name) {
      name
    }
  }
`);

type ArchiveEventModalProps = {
  eventName: string;
  isOpen: boolean;
  onClose: () => void;
};

export default function ArchiveEventModal({ eventName, isOpen, onClose }: ArchiveEventModalProps) {
  const environment = useEnvironment();
  const [error, setError] = useState<string>();
  const [{ fetching }, archiveEvent] = useMutation(ArchiveEvent);

  const handleSubmit = async () => {
    try {
      await archiveEvent({ name: eventName, environmentId: environment.id });
    } catch (error) {
      setError('Failed to archive event, please try again later.');
      console.error('error achiving event', eventName, error);
    }
  };

  return (
    <AlertModal
      className="w-1/3"
      isLoading={fetching}
      isOpen={isOpen}
      onClose={onClose}
      onSubmit={handleSubmit}
      title="Are you sure you want to archive this event?"
    >
      <p className="px-6 pt-4">
        Are you sure you want to archive this event? This action cannot be undone.
      </p>

      {error && (
        <Alert className="mt-6" severity="error">
          {error}
        </Alert>
      )}
    </AlertModal>
  );
}
