import { useState } from 'react';
import { Button } from '@inngest/components/Button';
import { Modal } from '@inngest/components/Modal';
import { toast } from 'sonner';

import Input from '@/components/Form/Input';
import useDebounce from '@/hooks/useDebounce';
import { IconExclamationTriangle } from '@/icons';
import { useCreateAppMutation } from '@/store/generated';
import isValidUrl from '@/utils/urlValidation';

type AddAppModalProps = {
  isOpen: boolean;
  onClose: () => void;
};

export default function AddAppModal({ isOpen, onClose }: AddAppModalProps) {
  const [inputUrl, setInputUrl] = useState('');
  const [isUrlInvalid, setUrlInvalid] = useState(false);
  const [isDisabled, setDisabled] = useState(true);
  const [_createApp, createAppState] = useCreateAppMutation();

  const debouncedRequest = useDebounce(() => {
    if (isValidUrl(inputUrl)) {
      setUrlInvalid(false);
    } else {
      setUrlInvalid(true);
    }
  });

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    setInputUrl(e.target.value);
    debouncedRequest();
    if (e.target.value.length > 0) {
      setDisabled(false);
    } else {
      setDisabled(true);
    }
  }

  async function createApp() {
    try {
      const response = await _createApp({
        input: {
          url: inputUrl,
        },
      });
      toast.success('The app was successfully added.');
      console.log('Created app:', response);
    } catch (error) {
      toast.error('The app could not be created: ${error}.');
      console.error('Error creating app:', error);
    }
    onClose();
  }

  function handleSubmit(e: React.SyntheticEvent) {
    e.preventDefault();
    createApp();
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleSubmit(e);
    }
  }

  return (
    <Modal isOpen={isOpen} onClose={onClose}>
      <Modal.Header description="Sync your Inngest application to the Dev Server">
        Sync App
      </Modal.Header>
      <Modal.Body>
        <form id="add-app" onSubmit={handleSubmit}>
          <div>
            <label htmlFor="addAppUrlModal" className="text-sm font-semibold text-white">
              App URL
              <span className="block pb-4 text-sm text-slate-500">The URL of your application</span>
            </label>
            <Input
              id="addAppUrlModal"
              value={inputUrl}
              placeholder="http://localhost:3000/api/inngest"
              onChange={handleChange}
              onKeyDown={handleKeyDown}
              isInvalid={isUrlInvalid}
            />
          </div>
          {isUrlInvalid && inputUrl.length > 0 && (
            <p className="flex items-center gap-2 bg-rose-600/50 px-6 py-2 text-sm text-white">
              <IconExclamationTriangle />
              Please enter a valid URL
            </p>
          )}
          {/* TODO(cdzombak): check for /ingest */}
        </form>
      </Modal.Body>
      <Modal.Footer className="flex justify-end gap-2">
        <Button label="Cancel" appearance="outlined" btnAction={onClose} />
        <Button
          disabled={isDisabled || isUrlInvalid}
          label="Sync App"
          type="submit"
          form="add-app"
        />
      </Modal.Footer>
    </Modal>
  );
}
