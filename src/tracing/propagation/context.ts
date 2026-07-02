import { context, propagation, ROOT_CONTEXT } from '@platform/tracing';

export class ContextPropagation {
  static createCarrier() {
    const carrier: Record<string, string> = {};

    propagation.inject(context.active(), carrier);

    return carrier;
  }

  static extractContext(carrier: Record<string, string>) {
    return propagation.extract(ROOT_CONTEXT, carrier);
  }
}
