'use client';

import { Button } from '@inngest/components/Button';

import { StatusMenu } from '@/components/Functions/StatusMenu';
import { FunctionTable } from './FunctionTable';
import { useRows } from './useRows';

type FunctionListProps = {
  envSlug: string;
  archived?: boolean;
};

export const FunctionList = ({ envSlug, archived }: FunctionListProps) => {
  const { error, isLoading, hasMore, loadMore, rows } = useRows({ archived: !!archived });
  if (error) {
    throw error;
  }

  return (
    <div className="bg-canvasBase flex min-h-0 flex-1 flex-col">
      <div className="mx-4 my-1 flex h-10 flex-row items-center justify-start">
        <StatusMenu archived={!!archived} envSlug={envSlug} />
      </div>

      <FunctionTable rows={rows} />

      {hasMore !== false && (
        <div className="flex w-full justify-center py-2.5">
          <Button
            loading={isLoading}
            appearance="outlined"
            btnAction={loadMore}
            label={isLoading ? 'Loading' : 'Load More'}
          />
        </div>
      )}
    </div>
  );
};
