import { readFile, writeFile, mkdir } from 'fs/promises';
import { existsSync } from 'fs';
import { join } from 'path';
import { WatermarkRecord } from '../types.js';

export class WatermarkTracker {
  private watermarksPath: string;
  private watermarks: Map<string, WatermarkRecord> = new Map();

  constructor(basePath: string = './etl-metadata') {
    this.watermarksPath = join(basePath, 'watermarks.json');
  }

  async load(): Promise<void> {
    try {
      if (existsSync(this.watermarksPath)) {
        const content = await readFile(this.watermarksPath, 'utf-8');
        const data = JSON.parse(content);
        
        for (const [source, record] of Object.entries(data)) {
          this.watermarks.set(source, {
            ...record as WatermarkRecord,
            lastWatermark: new Date((record as WatermarkRecord).lastWatermark),
            lastRunTime: new Date((record as WatermarkRecord).lastRunTime),
          });
        }
      }
    } catch (error) {
      console.warn(`Failed to load watermarks: ${error}`);
    }
  }

  async save(): Promise<void> {
    try {
      const dir = this.watermarksPath.substring(0, this.watermarksPath.lastIndexOf('/'));
      await mkdir(dir, { recursive: true });

      const data: Record<string, WatermarkRecord> = {};
      for (const [source, record] of this.watermarks.entries()) {
        data[source] = record;
      }

      await writeFile(this.watermarksPath, JSON.stringify(data, null, 2));
    } catch (error) {
      throw new Error(`Failed to save watermarks: ${error}`);
    }
  }

  getWatermark(source: string): Date | undefined {
    return this.watermarks.get(source)?.lastWatermark;
  }

  setWatermark(source: string, watermark: Date): void {
    this.watermarks.set(source, {
      source,
      lastWatermark: watermark,
      lastRunTime: new Date(),
    });
  }

  getLastRunTime(source: string): Date | undefined {
    return this.watermarks.get(source)?.lastRunTime;
  }
}
