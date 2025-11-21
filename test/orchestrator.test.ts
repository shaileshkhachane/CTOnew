import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ETLOrchestrator } from '../src/etl/orchestrator/orchestrator.js';
import { InMemoryStore } from '../src/etl/storage/store.js';
import { CubeManager } from '../src/cube/manager.js';
import { ETLConfig } from '../src/etl/types.js';
import { writeFile, mkdir } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

describe('ETLOrchestrator', () => {
  let testDir: string;
  let store: InMemoryStore;
  let cubeManager: CubeManager;

  beforeEach(async () => {
    testDir = join(tmpdir(), `etl-orchestrator-test-${Date.now()}`);
    await mkdir(testDir, { recursive: true });
    store = new InMemoryStore();
    cubeManager = new CubeManager();
  });

  it('should successfully process a complete ETL pipeline', async () => {
    const salesCsv = join(testDir, 'sales.csv');
    await writeFile(
      salesCsv,
      'id,product_id,customer_id,quantity,amount,sale_date,region\n' +
      '1,P001,C001,5,150.00,2024-01-15,North\n' +
      '2,P002,C002,3,225.00,2024-01-16,South'
    );

    const productsCsv = join(testDir, 'products.csv');
    await writeFile(
      productsCsv,
      'product_id,product_name,category,price,valid_from\n' +
      'P001,Widget A,Electronics,30.00,2024-01-01\n' +
      'P002,Gadget B,Electronics,75.00,2024-01-01'
    );

    const customersCsv = join(testDir, 'customers.csv');
    await writeFile(
      customersCsv,
      'customer_id,customer_name,email,segment,valid_from\n' +
      'C001,John Doe,john@example.com,Premium,2024-01-01\n' +
      'C002,Jane Smith,jane@example.com,Standard,2024-01-01'
    );

    const config: ETLConfig = {
      sources: [
        {
          name: 'sales_csv',
          type: 'csv',
          connection: { filePath: salesCsv, hasHeader: true },
          schema: 'sales',
        },
        {
          name: 'products_csv',
          type: 'csv',
          connection: { filePath: productsCsv, hasHeader: true },
          schema: 'product',
        },
        {
          name: 'customers_csv',
          type: 'csv',
          connection: { filePath: customersCsv, hasHeader: true },
          schema: 'customer',
        },
      ],
      target: { type: 'memory' },
      cubes: [
        {
          name: 'sales',
          factTable: 'fact_sales',
          dimensions: ['product', 'customer'],
          measures: ['quantity', 'amount'],
          sources: ['sales_csv', 'products_csv', 'customers_csv'],
        },
      ],
    };

    const orchestrator = new ETLOrchestrator({
      config,
      store,
      cubeManager,
      metadataPath: join(testDir, 'metadata'),
    });

    const results = await orchestrator.run('sales');

    expect(results).toHaveLength(3);
    expect(results.every(r => r.status === 'success')).toBe(true);
    expect(results.find(r => r.source === 'sales_csv')?.rowsProcessed).toBe(2);

    const facts = await store.getFacts('fact_sales');
    expect(facts).toHaveLength(2);

    const products = await store.getDimensions('dim_product');
    expect(products).toHaveLength(2);

    const customers = await store.getDimensions('dim_customer');
    expect(customers).toHaveLength(2);
  });

  it('should handle validation failures', async () => {
    const salesCsv = join(testDir, 'invalid-sales.csv');
    await writeFile(
      salesCsv,
      'id,product_id,customer_id,quantity,amount,sale_date,region\n' +
      '1,P001,,5,150.00,2024-01-15,North'
    );

    const config: ETLConfig = {
      sources: [
        {
          name: 'sales_csv',
          type: 'csv',
          connection: { filePath: salesCsv, hasHeader: true },
          schema: 'sales',
        },
      ],
      target: { type: 'memory' },
      cubes: [
        {
          name: 'sales',
          factTable: 'fact_sales',
          dimensions: [],
          measures: ['quantity', 'amount'],
          sources: ['sales_csv'],
        },
      ],
    };

    const orchestrator = new ETLOrchestrator({
      config,
      store,
      cubeManager,
      metadataPath: join(testDir, 'metadata'),
    });

    const results = await orchestrator.run('sales');

    expect(results).toHaveLength(1);
    expect(results[0].status).toBe('failed');
    expect(results[0].errors).toBeDefined();
    expect(results[0].errors!.length).toBeGreaterThan(0);

    const facts = await store.getFacts('fact_sales');
    expect(facts).toHaveLength(0);
  });

  it('should trigger cube invalidation', async () => {
    const salesCsv = join(testDir, 'sales.csv');
    await writeFile(
      salesCsv,
      'id,product_id,customer_id,quantity,amount,sale_date,region\n' +
      '1,P001,C001,5,150.00,2024-01-15,North'
    );

    const config: ETLConfig = {
      sources: [
        {
          name: 'sales_csv',
          type: 'csv',
          connection: { filePath: salesCsv, hasHeader: true },
          schema: 'sales',
        },
      ],
      target: { type: 'memory' },
      cubes: [
        {
          name: 'sales',
          factTable: 'fact_sales',
          dimensions: [],
          measures: ['quantity', 'amount'],
          sources: ['sales_csv'],
        },
      ],
    };

    const invalidationSpy = vi.fn();
    cubeManager.onInvalidation(invalidationSpy);

    const orchestrator = new ETLOrchestrator({
      config,
      store,
      cubeManager,
      metadataPath: join(testDir, 'metadata'),
    });

    await orchestrator.run('sales');

    expect(invalidationSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        cube: 'sales',
        sources: ['sales_csv'],
      })
    );
  });

  it('should process incremental updates with watermarks', async () => {
    const salesCsv = join(testDir, 'sales-incremental.csv');
    await writeFile(
      salesCsv,
      'id,product_id,customer_id,quantity,amount,sale_date,region\n' +
      '1,P001,C001,5,150.00,2024-01-15,North\n' +
      '2,P002,C002,3,225.00,2024-01-20,South'
    );

    const config: ETLConfig = {
      sources: [
        {
          name: 'sales_csv',
          type: 'csv',
          connection: { filePath: salesCsv, hasHeader: true },
          schema: 'sales',
          watermarkColumn: 'sale_date',
        },
      ],
      target: { type: 'memory' },
      cubes: [
        {
          name: 'sales',
          factTable: 'fact_sales',
          dimensions: [],
          measures: ['quantity', 'amount'],
          sources: ['sales_csv'],
        },
      ],
    };

    const orchestrator = new ETLOrchestrator({
      config,
      store,
      cubeManager,
      metadataPath: join(testDir, 'metadata'),
      since: new Date('2024-01-18'),
    });

    const results = await orchestrator.run('sales');

    expect(results[0].status).toBe('success');
    expect(results[0].rowsProcessed).toBe(1);

    const facts = await store.getFacts('fact_sales');
    expect(facts).toHaveLength(1);
    expect(facts[0].id).toBe(2);
  });
});
