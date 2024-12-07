import { MetadataGrid, type MetadataItemProps } from '@inngest/components/Metadata';
import { noCase } from 'change-case';
import { titleCase } from 'title-case';

import Block from '@/components/Block';
import type { FunctionConfiguration } from '@/gql/graphql';

type FunctionConfigurationProps = {
  configuration: FunctionConfiguration;
};

export default function FunctionConfiguration({ configuration }: FunctionConfigurationProps) {
  const miscellaneousItems: MetadataItemProps[] = [
    {
      size: 'large',
      label: 'Retries',
      value: configuration.retries.value.toString(),
      ...(configuration.retries.isDefault && {
        badge: {
          label: 'Default',
        },
      }),
      tooltip: 'The number of times the function will be retried when it errors.',
    },
  ];

  if (configuration.priority) {
    miscellaneousItems.push({
      label: 'Priority',
      value: configuration.priority,
      size: 'large',
      type: 'code',
      tooltip:
        'When the function is triggered multiple times simultaneously, the priority determines the order in which they are run.' +
        '\n\n' +
        'The priority value is determined by evaluating the configured expression. The higher the value, the higher the priority.',
    });
  }

  let rateLimitItems: MetadataItemProps[] | undefined;
  if (configuration.rateLimit) {
    rateLimitItems = [
      {
        label: 'Period',
        value: configuration.rateLimit.period,
      },
      { label: 'Limit', value: configuration.rateLimit.limit.toString() },
    ];

    if (configuration.rateLimit.key) {
      rateLimitItems.push({
        label: 'Key',
        value: configuration.rateLimit.key,
        type: 'code',
        size: 'large',
      });
    }
  }

  let debounceItems: MetadataItemProps[] | undefined;
  if (configuration.debounce) {
    debounceItems = [
      {
        label: 'Period',
        value: configuration.debounce.period,
      },
    ];

    if (configuration.debounce.key) {
      debounceItems.push({
        label: 'Key',
        value: configuration.debounce.key,
        type: 'code',
      });
    }
  }

  let eventBatchItems: MetadataItemProps[] | undefined;
  if (configuration.eventsBatch) {
    eventBatchItems = [
      {
        label: 'Max Size',
        value: configuration.eventsBatch.maxSize.toString(),
      },
      {
        label: 'Timeout',
        value: configuration.eventsBatch.timeout,
      },
    ];

    if (configuration.eventsBatch.key) {
      eventBatchItems.push({
        label: 'Key',
        value: configuration.eventsBatch.key,
        type: 'code',
      });
    }
  }

  let throttleItems: MetadataItemProps[] | undefined;
  if (configuration.throttle) {
    throttleItems = [
      {
        label: 'Period',
        value: configuration.throttle.period,
      },
      {
        label: 'Limit',
        value: configuration.throttle.limit.toString(),
      },
      {
        label: 'Burst',
        value: configuration.throttle.burst.toString(),
      },
    ];

    if (configuration.throttle.key) {
      throttleItems.push({
        label: 'Key',
        value: configuration.throttle.key,
        type: 'code',
      });
    }
  }

  return (
    <Block title="Configuration">
      <MetadataGrid columns={2} metadataItems={miscellaneousItems} />
      {eventBatchItems && (
        <>
          <h3 className="text-basis pb-2 pt-6 text-sm font-medium">Events Batch</h3>
          <MetadataGrid columns={2} metadataItems={eventBatchItems} />
        </>
      )}
      <h3 className="text-basis pb-2 pt-6 text-sm font-medium">Concurrency</h3>
      <div className="space-y-3">
        {configuration.concurrency.map((concurrencyItem, index) => {
          const items: MetadataItemProps[] = [
            {
              label: 'Scope',
              value: titleCase(noCase(concurrencyItem.scope)),
            },
            {
              label: 'Limit',
              value: `${concurrencyItem.limit.value}`,
              ...(concurrencyItem.limit.isPlanLimit && {
                badge: {
                  label: 'Plan Limit',
                  description:
                    'If not configured, the limit is set to the maximum value allowed within your plan.',
                },
              }),
              tooltip: 'The maximum number of concurrently running steps.',
            },
          ];

          if (concurrencyItem.key) {
            items.push({
              label: 'Key',
              value: concurrencyItem.key,
              type: 'code',
              size: 'large',
            });
          }
          return <MetadataGrid key={index} columns={2} metadataItems={items} />;
        })}
      </div>
      {rateLimitItems && (
        <>
          <h3 className="text-basis pb-2 pt-6 text-sm font-medium">Rate Limit</h3>
          <MetadataGrid columns={2} metadataItems={rateLimitItems} />
        </>
      )}
      {debounceItems && (
        <>
          <h3 className="text-basis pb-2 pt-6 text-sm font-medium">Debounce</h3>
          <MetadataGrid columns={2} metadataItems={debounceItems} />
        </>
      )}
      {throttleItems && (
        <>
          <h3 className="text-basis pb-2 pt-6 text-sm font-medium">Throttle</h3>
          <MetadataGrid columns={2} metadataItems={throttleItems} />
        </>
      )}
    </Block>
  );
}
