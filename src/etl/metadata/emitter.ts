import { writeFile, mkdir } from 'fs/promises';
import { join } from 'path';
import { RefreshMetadata } from '../types.js';

export class MetadataEmitter {
  constructor(private basePath: string = './etl-metadata') {}

  async emit(metadata: RefreshMetadata): Promise<void> {
    await mkdir(this.basePath, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `${metadata.cube}_${metadata.source}_${timestamp}.json`;
    const filepath = join(this.basePath, filename);

    await writeFile(filepath, JSON.stringify(metadata, null, 2));

    const latestPath = join(this.basePath, `${metadata.cube}_${metadata.source}_latest.json`);
    await writeFile(latestPath, JSON.stringify(metadata, null, 2));
  }

  async emitSummary(allMetadata: RefreshMetadata[]): Promise<void> {
    await mkdir(this.basePath, { recursive: true });

    const summary = {
      timestamp: new Date().toISOString(),
      totalRuns: allMetadata.length,
      successful: allMetadata.filter(m => m.status === 'success').length,
      failed: allMetadata.filter(m => m.status === 'failed').length,
      partial: allMetadata.filter(m => m.status === 'partial').length,
      totalRowsProcessed: allMetadata.reduce((sum, m) => sum + m.rowsProcessed, 0),
      totalRowsInserted: allMetadata.reduce((sum, m) => sum + m.rowsInserted, 0),
      totalRowsUpdated: allMetadata.reduce((sum, m) => sum + m.rowsUpdated, 0),
      runs: allMetadata,
    };

    const filepath = join(this.basePath, 'summary.json');
    await writeFile(filepath, JSON.stringify(summary, null, 2));
  }
}
