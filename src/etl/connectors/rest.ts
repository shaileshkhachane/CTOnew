import { BaseConnector } from './base.js';
import { RESTConnection, StagingData } from '../types.js';

export class RESTConnector extends BaseConnector {
  constructor(
    private config: RESTConnection,
    private schema: string,
    private sourceName: string,
    private watermarkColumn?: string
  ) {
    super();
  }

  async connect(): Promise<void> {
    try {
      const response = await fetch(this.config.url, {
        method: 'HEAD',
        headers: this.buildHeaders(),
      });
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      
      this.connected = true;
    } catch (error) {
      throw new Error(`Failed to connect to REST API: ${error}`);
    }
  }

  async extract(watermark?: Date): Promise<StagingData> {
    this.ensureConnected();

    const url = new URL(this.config.url);
    if (watermark && this.watermarkColumn) {
      url.searchParams.append(this.watermarkColumn, watermark.toISOString());
    }

    const response = await fetch(url.toString(), {
      method: this.config.method || 'GET',
      headers: this.buildHeaders(),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const json = await response.json();
    const data = this.config.dataPath 
      ? this.extractDataFromPath(json, this.config.dataPath)
      : Array.isArray(json) ? json : [json];

    let maxWatermark: Date | undefined;
    if (this.watermarkColumn && data.length > 0) {
      const watermarks = data
        .map((row: any) => new Date(row[this.watermarkColumn!]))
        .filter((date: Date) => !isNaN(date.getTime()));
      
      if (watermarks.length > 0) {
        maxWatermark = new Date(Math.max(...watermarks.map((d: Date) => d.getTime())));
      }
    }

    return {
      source: this.sourceName,
      schema: this.schema,
      data,
      watermark: maxWatermark,
    };
  }

  async disconnect(): Promise<void> {
    this.connected = false;
  }

  private buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...this.config.headers,
    };

    if (this.config.auth) {
      if (this.config.auth.type === 'bearer' && this.config.auth.token) {
        headers['Authorization'] = `Bearer ${this.config.auth.token}`;
      } else if (this.config.auth.type === 'basic' && this.config.auth.username && this.config.auth.password) {
        const encoded = Buffer.from(`${this.config.auth.username}:${this.config.auth.password}`).toString('base64');
        headers['Authorization'] = `Basic ${encoded}`;
      }
    }

    return headers;
  }

  private extractDataFromPath(obj: any, path: string): any[] {
    const keys = path.split('.');
    let current = obj;
    
    for (const key of keys) {
      if (current && typeof current === 'object' && key in current) {
        current = current[key];
      } else {
        return [];
      }
    }
    
    return Array.isArray(current) ? current : [current];
  }
}
