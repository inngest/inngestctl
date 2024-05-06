import { useState } from 'react';
import { Badge } from '@inngest/components/Badge';
import { Button } from '@inngest/components/Button';
import { CodeLine } from '@inngest/components/CodeLine';
import { Link } from '@inngest/components/Link/Link';
import { AlertModal } from '@inngest/components/Modal';
import { IconStatusCanceled } from '@inngest/components/icons/status/Canceled';
import { IconStatusCompleted } from '@inngest/components/icons/status/Completed';
import { IconStatusFailed } from '@inngest/components/icons/status/Failed';
import { cn } from '@inngest/components/utils/classNames';
import { RiArrowDownSLine } from '@remixicon/react';
import { toast } from 'sonner';

import AppCardHeader from '@/components/App/AppCardHeader';
import useDebounce from '@/hooks/useDebounce';
import { IconSpinner } from '@/icons';
import {
  useDeleteAppMutation,
  useUpdateAppMutation,
  type App as ApiApp,
  type Function,
} from '@/store/generated';
import isValidUrl from '@/utils/urlValidation';
import AppCardStep from './AppCardStep';

type App = Pick<
  ApiApp,
  | 'autodiscovered'
  | 'connected'
  | 'error'
  | 'framework'
  | 'functionCount'
  | 'id'
  | 'name'
  | 'sdkLanguage'
  | 'sdkVersion'
  | 'url'
> & { functions: Pick<Function, 'id' | 'name'>[] };

export default function AppCard({ app }: { app: App }) {
  const [inputUrl, setInputUrl] = useState(app.url || '');
  const [isUrlInvalid, setUrlInvalid] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [isAlertModalOpen, setIsAlertModalOpen] = useState(false);
  const [_updateApp, updateAppState] = useUpdateAppMutation();
  const [_deleteApp, deleteAppState] = useDeleteAppMutation();

  const debouncedRequest = useDebounce(() => {
    if (isValidUrl(inputUrl)) {
      setUrlInvalid(false);
      updateApp();
    } else {
      setUrlInvalid(true);
      setIsLoading(false);
    }
  });

  async function updateApp() {
    try {
      const response = await _updateApp({
        input: {
          url: inputUrl,
          id: app.id,
        },
      });
      toast.success('The URL was successfully updated.');
      console.log('Edited app URL:', response);
    } catch (error) {
      toast.error('The URL could not be updated.');
      console.error('Error editing app:', error);
    }
    setIsLoading(false);
  }

  async function deleteApp() {
    try {
      const response = await _deleteApp({
        id: app.id,
      });
      toast.success(`${app.name || 'The app'} was successfully deleted.`);
      console.log('Deleted app:', response);
    } catch (error) {
      toast.error(`${app.name || 'The app'} could not be deleted: ${error}`);
      console.error('Error deleting app:', error);
    }
    // To do: add optimistic render in the list
  }

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    setInputUrl(e.target.value);
    setIsLoading(true);
    debouncedRequest();
  }

  function handleDelete() {
    deleteApp();
  }

  return (
    <div>
      <AppCardHeader synced={app.connected} functionCount={app.functionCount} />
      <div className="divide-y divide-slate-700/30 rounded-b-md border border-slate-700/30 bg-slate-800/30">
        {!app.name ? (
          <div className="flex items-center gap-2 p-4 pr-6">
            <IconSpinner />
            <p className="text-lg font-normal text-slate-400">Syncing...</p>
          </div>
        ) : (
          <div className="flex items-center justify-between px-6 py-4 ">
            {!app.connected ? (
              <div className="flex items-center gap-2">
                <IconSpinner />
                <p className="text-lg font-normal text-slate-400">Syncing to {app.name}...</p>
              </div>
            ) : (
              <p className=" text-lg text-white">{app.name}</p>
            )}
            {app.autodiscovered && <Badge>Auto Detected</Badge>}
          </div>
        )}
        <AppCardStep
          lineContent={
            <>
              <div className="">
                <div className="flex items-center gap-3 text-base">
                  {app.connected ? (
                    <>{<IconStatusCompleted />}Synced to app</>
                  ) : (
                    <>{<IconStatusFailed />}Not synced to app</>
                  )}
                </div>
                <p className="ui-open:hidden pl-10 text-slate-300 xl:hidden">{app.url}</p>
              </div>
              <div className="flex items-center gap-4">
                <p className="xl:ui-open:hidden hidden text-slate-300 xl:flex">{app.url}</p>
                <RiArrowDownSLine className="ui-open:-rotate-180 transform-90 text-slate-500 transition-transform duration-500" />
              </div>
            </>
          }
          expandedContent={
            <>
              {!app.connected && (
                <>
                  <p className="pb-4 text-slate-400">
                    The Inngest Dev Server can&apos;t find your application. Ensure your full URL is
                    correct, including the correct port. Inngest automatically scans{' '}
                    <span className="text-white">multiple ports</span> by default.
                  </p>
                  {app.error && (
                    <p className="pb-4 font-medium text-rose-400">Error: {app.error}</p>
                  )}
                </>
              )}
              {app.url && app.url.endsWith('/ingest') && (
                <p className="pb-4 font-medium text-rose-400">
                  You're using an Inngest endpoint at <code>{app.url}</code>, but{' '}
                  <code>inngest</code>
                  is spelled wrong -- please spell it <code>/inngest</code>.
                </p>
              )}
              <form className="block pb-4 xl:flex xl:items-center xl:justify-between">
                <label htmlFor="editAppUrl" className="text-sm font-semibold text-white">
                  App URL
                  <span className="block text-sm font-normal text-slate-400">
                    The URL of your application
                  </span>
                </label>
                <div className="relative flex-1 pt-2 xl:pl-10 xl:pt-0">
                  <input
                    id="editAppUrl"
                    className={cn(
                      'w-full rounded-md bg-slate-800 px-4 py-2 text-slate-300 outline-2 outline-indigo-500 read-only:outline-transparent focus:outline',
                      isUrlInvalid && ' outline-rose-400',
                      isLoading && 'pr-6'
                    )}
                    value={inputUrl}
                    placeholder="http://localhost:3000/api/inngest"
                    onChange={handleChange}
                    readOnly={app.autodiscovered}
                  />
                  {isLoading && <IconSpinner className="absolute right-2 top-1/3" />}
                  {isUrlInvalid && (
                    <p className="absolute left-14 top-10 text-rose-400">
                      Please enter a valid URL
                    </p>
                  )}
                  {/* TODO(cdzombak): check for /ingest */}
                </div>
              </form>
              <div className="mb-4 grid grid-cols-3 border-y border-slate-700/30">
                <div className="py-4">
                  <p className="text-sm font-semibold text-white">{app.framework || '-'}</p>
                  <p className="text-sm text-slate-400">Framework</p>
                </div>
                <div className="py-4">
                  <p className="text-sm font-semibold text-white">{app.sdkLanguage || '-'}</p>
                  <p className="text-sm text-slate-400">Language</p>
                </div>
                <div className="py-4">
                  <p className="text-sm font-semibold text-white">{app.sdkVersion || '-'}</p>
                  <p className="text-sm text-slate-400">SDK Version</p>
                </div>
              </div>
              {!app.connected && (
                <Link className="w-fit" href="https://www.inngest.com/docs/sdk/serve">
                  Syncing to the Dev Server
                </Link>
              )}
            </>
          }
        />
        <AppCardStep
          lineContent={
            <>
              <div className="flex items-center gap-3 text-base">
                {app.functionCount > 0 && (
                  <>
                    {app.connected && <IconStatusCompleted />}
                    {!app.connected && <IconStatusCanceled />}
                    {app.functionCount} function
                    {app.functionCount === 1 ? '' : 's'} registered
                  </>
                )}
                {app.functionCount < 1 && (
                  <>
                    <IconStatusCanceled />
                    No functions found
                  </>
                )}
              </div>
              <div className="flex items-center gap-4">
                {app.functionCount > 0 && (
                  <Link internalNavigation href="/functions">
                    View Functions
                  </Link>
                )}
                <RiArrowDownSLine className="ui-open:-rotate-180 transform-90 text-slate-500 transition-transform duration-500" />
              </div>
            </>
          }
          expandedContent={
            <>
              {app.functionCount < 1 && (
                <>
                  <p className="pb-4 text-slate-400">
                    There are currently no functions registered at this URL. Ensure you have created
                    a function and are exporting it correctly from your serve command.
                  </p>
                  <CodeLine code="serve(client, [list_of_fns]);" className="mb-4" />
                  <Link className="w-fit" href="https://www.inngest.com/docs/functions">
                    Creating Functions
                  </Link>
                </>
              )}
              {app.functionCount > 0 && (
                <ul className="columns-2">
                  {[...app.functions]
                    .sort((a, b) => a.name.localeCompare(b.name))
                    .map((func) => {
                      return (
                        <li key={func.id} className="py-1 text-slate-400">
                          {func.name}
                        </li>
                      );
                    })}
                </ul>
              )}
            </>
          }
        />
        {!app.autodiscovered && (
          <div className="p-4 pr-6 text-white">
            <AlertModal
              isOpen={isAlertModalOpen}
              title="Are you sure you want to delete the app?"
              onClose={() => setIsAlertModalOpen(false)}
              onSubmit={handleDelete}
            />
            <Button
              kind="danger"
              appearance="text"
              btnAction={() => setIsAlertModalOpen(true)}
              label="Delete App"
            />
          </div>
        )}
      </div>
    </div>
  );
}
