import pg from 'pg';
import { BaseConnector } from './base.js';
import { PostgresConnection, StagingData } from '../types.js';

const { Pool } = pg;

export class PostgresConnector extends BaseConnector {
  private pool: pg.Pool | null = null;

  constructor(
    private config: PostgresConnection,
    private schema: string,
    private sourceName: string,
    private watermarkColumn?: string
  ) {
    super();
  }

  async connect(): Promise<void> {
    this.pool = new Pool({
      host: this.config.host,
      port: this.config.port,
      database: this.config.database,
      user: this.config.user,
      password: this.config.password,
    });

    try {
      await this.pool.query('SELECT 1');
      this.connected = true;
    } catch (error) {
      throw new Error(`Failed to connect to PostgreSQL: ${error}`);
    }
  }

  async extract(watermark?: Date): Promise<StagingData> {
    this.ensureConnected();
    if (!this.pool) {
      throw new Error('Pool not initialized');
    }

    let query = this.config.query || `SELECT * FROM ${this.config.table}`;
    const params: any[] = [];

    if (watermark && this.watermarkColumn) {
      if (query.includes('WHERE')) {
        query += ` AND ${this.watermarkColumn} > $1`;
      } else {
        query += ` WHERE ${this.watermarkColumn} > $1`;
      }
      params.push(watermark);
    }

    if (this.watermarkColumn && !query.toLowerCase().includes('order by')) {
      query += ` ORDER BY ${this.watermarkColumn} ASC`;
    }

    const result = await this.pool.query(query, params);
    const data = result.rows;

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
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
    this.connected = false;
  }
}
