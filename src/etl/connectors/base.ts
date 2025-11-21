import { StagingData } from '../types.js';

export interface Connector {
  connect(): Promise<void>;
  extract(watermark?: Date): Promise<StagingData>;
  disconnect(): Promise<void>;
  healthCheck(): Promise<boolean>;
}

export abstract class BaseConnector implements Connector {
  protected connected: boolean = false;

  abstract connect(): Promise<void>;
  abstract extract(watermark?: Date): Promise<StagingData>;
  abstract disconnect(): Promise<void>;

  async healthCheck(): Promise<boolean> {
    return this.connected;
  }

  protected ensureConnected(): void {
    if (!this.connected) {
      throw new Error('Connector not connected. Call connect() first.');
    }
  }
}
