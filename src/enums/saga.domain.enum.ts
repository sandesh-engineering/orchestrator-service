import { logger } from '@platform/logger';

export enum SagaDomain {
  DISPATCH = 'dispatch',
  SUBSCRIPTION = 'subscription',
}

export const SAGA_DOMAIN_MAP: Record<string, SagaDomain> = {
  OrderPlacementSaga: SagaDomain.DISPATCH,
  OrderFulfillmentSaga: SagaDomain.DISPATCH,

  SubscriptionRenewalSaga: SagaDomain.SUBSCRIPTION,
  SubscriptionPaymentSaga: SagaDomain.SUBSCRIPTION,
};

export function resolveSagaDomain(sagaName: string): SagaDomain {
  const domain = SAGA_DOMAIN_MAP[sagaName];

  if (!domain) {
    logger.error(
      `No domain mapping configured for saga "${sagaName}". Add it to SAGA_DOMAIN_MAP in saga-domain.enum.ts.`,
      { sagaName },
    );
    throw new Error(
      `No domain mapping configured for saga "${sagaName}". Add it to SAGA_DOMAIN_MAP in saga-domain.enum.ts.`,
    );
  }

  return domain;
}
