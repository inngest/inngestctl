'use client';

import { Listbox } from '@headlessui/react';
import { NewButton } from '@inngest/components/Button';
import { RiArchive2Line, RiMore2Line } from '@remixicon/react';

export type EventActions = {
  archive: () => void;
};

export const ActionsMenu = ({ archive }: EventActions) => {
  return (
    <Listbox>
      <Listbox.Button as="div">
        <NewButton kind="primary" appearance="outlined" size="medium" icon={<RiMore2Line />} />
      </Listbox.Button>
      <div className="relative">
        <Listbox.Options className="bg-canvasBase absolute right-1 top-5 z-50 w-[170px] gap-y-0.5 rounded border shadow">
          <Listbox.Option
            className="m-2 flex h-8 cursor-pointer items-center text-[13px]"
            value="signingKeys"
          >
            <NewButton
              appearance="ghost"
              kind="danger"
              size="medium"
              icon={<RiArchive2Line className="h-4 w-4" />}
              iconSide="left"
              label={'Archive event'}
              className="m-0 w-full justify-start"
              onClick={archive}
            />
          </Listbox.Option>
        </Listbox.Options>
      </div>
    </Listbox>
  );
};
