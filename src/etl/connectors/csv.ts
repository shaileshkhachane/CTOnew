import { parse } from 'csv-parse/sync';
import { readFile } from 'fs/promises';
import { BaseConnector } from './base.js';
import { CSVConnection, StagingData } from '../types.js';

export class CSVConnector extends BaseConnector {
  constructor(
    private config: CSVConnection,
    private schema: string,
    private sourceName: string,
    private watermarkColumn?: string
  ) {
    super();
  }

  async connect(): Promise<void> {
    try {
      await readFile(this.config.filePath);
      this.connected = true;
    } catch (error) {
      throw new Error(`Failed to access CSV file: ${this.config.filePath}`);
    }
  }

  async extract(watermark?: Date): Promise<StagingData> {
    this.ensureConnected();

    const content = await readFile(this.config.filePath, 'utf-8');
    
    const records = parse(content, {
      columns: this.config.hasHeader !== false,
      skip_empty_lines: true,
      delimiter: this.config.delimiter || ',',
      cast: true,
      cast_date: false,
    });

    let filteredData = records;
    let maxWatermark: Date | undefined;

    if (watermark && this.watermarkColumn) {
      filteredData = records.filter((row: any) => {
        const rowDate = new Date(row[this.watermarkColumn!]);
        return rowDate > watermark;
      });
    }

    if (this.watermarkColumn && filteredData.length > 0) {
      const watermarks = filteredData
        .map((row: any) => new Date(row[this.watermarkColumn!]))
        .filter((date: Date) => !isNaN(date.getTime()));
      
      if (watermarks.length > 0) {
        maxWatermark = new Date(Math.max(...watermarks.map((d: Date) => d.getTime())));
      }
    }

    return {
      source: this.sourceName,
      schema: this.schema,
      data: filteredData,
      watermark: maxWatermark,
    };
  }

  async disconnect(): Promise<void> {
    this.connected = false;
  }
}
