'use client';

import { Alert } from '@inngest/components/Alert';
import { IconApp } from '@inngest/components/icons/App';

import { ArchivedAppBanner } from '@/components/ArchivedAppBanner';
import { useEnvironment } from '@/components/Environments/environment-context';
import { useBooleanFlag } from '@/components/FeatureFlags/hooks';
import OldHeader, { type HeaderLink } from '@/components/Header/old/Header';
import { ArchiveButton } from './ArchiveButton';
import { ResyncButton } from './ResyncButton';
import { ValidateButton } from './ValidateButton';
import { useNavData } from './useNavData';

type Props = React.PropsWithChildren<{
  params: {
    externalID: string;
  };
}>;

export default function OldLayout({ children, params: { externalID } }: Props) {
  const isAppValidationEnabled = useBooleanFlag('app-validation');
  externalID = decodeURIComponent(externalID);
  const env = useEnvironment();

  const res = useNavData({
    envID: env.id,
    externalAppID: externalID,
  });
  if (res.error) {
    if (res.error.message.includes('no rows')) {
      return (
        <div className="mt-4 flex place-content-center">
          <Alert severity="warning">{externalID} app not found in this environment</Alert>
        </div>
      );
    }
    throw res.error;
  }
  if (res.isLoading && !res.data) {
    return null;
  }

  const navLinks: HeaderLink[] = [
    {
      active: 'exact',
      href: `/env/${env.slug}/apps/${encodeURIComponent(externalID)}`,
      text: 'Info',
    },
    {
      active: 'exact',
      href: `/env/${env.slug}/apps/${encodeURIComponent(externalID)}/syncs`,
      text: 'Syncs',
    },
  ];

  const actions = [];
  if (res.data.latestSync?.url) {
    actions.push(
      <ResyncButton
        appExternalID={externalID}
        disabled={res.data.isArchived}
        platform={res.data.latestSync.platform}
        latestSyncUrl={res.data.latestSync.url}
      />
    );

    if (isAppValidationEnabled.value) {
      actions.push(<ValidateButton latestSyncUrl={res.data.latestSync.url} />);
    }
  }

  actions.push(
    <ArchiveButton
      appID={res.data.id}
      disabled={res.data.isParentArchived}
      isArchived={res.data.isArchived}
    />
  );

  return (
    <>
      <ArchivedAppBanner externalAppID={externalID} />
      <OldHeader
        action={<div className="flex gap-4">{actions}</div>}
        icon={<IconApp className="h-5 w-5 text-white" />}
        links={navLinks}
        title={res.data.name}
      />
      <div className="h-full overflow-hidden bg-slate-100">{children}</div>
    </>
  );
}