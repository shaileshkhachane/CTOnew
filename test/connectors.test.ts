import { describe, it, expect, beforeEach, vi } from 'vitest';
import { CSVConnector } from '../src/etl/connectors/csv.js';
import { writeFile, mkdir } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

describe('CSVConnector', () => {
  let testDir: string;

  beforeEach(async () => {
    testDir = join(tmpdir(), `etl-test-${Date.now()}`);
    await mkdir(testDir, { recursive: true });
  });

  it('should extract data from CSV file', async () => {
    const csvPath = join(testDir, 'test.csv');
    const csvContent = `id,name,value,date\n1,Item1,100,2024-01-01\n2,Item2,200,2024-01-02`;
    await writeFile(csvPath, csvContent);

    const connector = new CSVConnector(
      { filePath: csvPath, hasHeader: true },
      'test',
      'test_source'
    );

    await connector.connect();
    const result = await connector.extract();
    await connector.disconnect();

    expect(result.data).toHaveLength(2);
    expect(result.data[0]).toMatchObject({ id: 1, name: 'Item1', value: 100 });
    expect(result.schema).toBe('test');
    expect(result.source).toBe('test_source');
  });

  it('should filter by watermark', async () => {
    const csvPath = join(testDir, 'watermark-test.csv');
    const csvContent = `id,name,date\n1,Item1,2024-01-01\n2,Item2,2024-01-15\n3,Item3,2024-01-20`;
    await writeFile(csvPath, csvContent);

    const connector = new CSVConnector(
      { filePath: csvPath, hasHeader: true },
      'test',
      'test_source',
      'date'
    );

    await connector.connect();
    const watermark = new Date('2024-01-10');
    const result = await connector.extract(watermark);
    await connector.disconnect();

    expect(result.data).toHaveLength(2);
    expect(result.data[0].id).toBe(2);
    expect(result.watermark).toBeDefined();
  });
});
