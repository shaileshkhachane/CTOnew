import { describe, it, expect, beforeEach } from 'vitest';
import { ETLOrchestrator } from '../src/etl/orchestrator/orchestrator.js';
import { InMemoryStore } from '../src/etl/storage/store.js';
import { CubeManager } from '../src/cube/manager.js';
import { ETLConfig } from '../src/etl/types.js';
import { writeFile, mkdir, rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

describe('Integration Tests', () => {
  let testDir: string;

  beforeEach(async () => {
    testDir = join(tmpdir(), `etl-integration-${Date.now()}`);
    await mkdir(testDir, { recursive: true });
    await mkdir(join(testDir, 'data'), { recursive: true });
  });

  it('should run full ETL pipeline with CSV sources', async () => {
    const salesCsv = join(testDir, 'data', 'sales.csv');
    await writeFile(
      salesCsv,
      'id,product_id,customer_id,quantity,amount,sale_date,region\n' +
      '1,P001,C001,5,150.00,2024-01-15,North\n' +
      '2,P002,C002,3,225.00,2024-01-16,South\n' +
      '3,P001,C003,2,60.00,2024-01-17,East'
    );

    const productsCsv = join(testDir, 'data', 'products.csv');
    await writeFile(
      productsCsv,
      'product_id,product_name,category,price,valid_from\n' +
      'P001,Widget A,Electronics,30.00,2024-01-01\n' +
      'P002,Gadget B,Electronics,75.00,2024-01-01'
    );

    const customersCsv = join(testDir, 'data', 'customers.csv');
    await writeFile(
      customersCsv,
      'customer_id,customer_name,email,segment,valid_from\n' +
      'C001,John Doe,john@example.com,Premium,2024-01-01\n' +
      'C002,Jane Smith,jane@example.com,Standard,2024-01-01\n' +
      'C003,Bob Johnson,bob@example.com,Premium,2024-01-01'
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
        {
          name: 'products_csv',
          type: 'csv',
          connection: { filePath: productsCsv, hasHeader: true },
          schema: 'product',
          watermarkColumn: 'valid_from',
        },
        {
          name: 'customers_csv',
          type: 'csv',
          connection: { filePath: customersCsv, hasHeader: true },
          schema: 'customer',
          watermarkColumn: 'valid_from',
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

    const store = new InMemoryStore();
    const cubeManager = new CubeManager();

    const orchestrator = new ETLOrchestrator({
      config,
      store,
      cubeManager,
      metadataPath: join(testDir, 'metadata'),
    });

    const results = await orchestrator.run('sales');

    expect(results).toHaveLength(3);
    expect(results.every(r => r.status === 'success')).toBe(true);

    const facts = await store.getFacts('fact_sales');
    expect(facts).toHaveLength(3);
    expect(facts[0].id).toBe(1);

    const products = await store.getDimensions('dim_product');
    expect(products).toHaveLength(2);
    expect(products.every(p => p.is_current)).toBe(true);

    const customers = await store.getDimensions('dim_customer');
    expect(customers).toHaveLength(3);
  });

  it('should handle incremental refresh correctly', async () => {
    const salesCsv = join(testDir, 'data', 'sales.csv');
    await writeFile(
      salesCsv,
      'id,product_id,customer_id,quantity,amount,sale_date,region\n' +
      '1,P001,C001,5,150.00,2024-01-15,North\n' +
      '2,P002,C002,3,225.00,2024-01-16,South\n' +
      '3,P001,C003,2,60.00,2024-01-25,East\n' +
      '4,P002,C001,1,75.00,2024-01-26,North'
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

    const store = new InMemoryStore();

    const orchestrator1 = new ETLOrchestrator({
      config,
      store,
      metadataPath: join(testDir, 'metadata'),
      since: new Date('2024-01-20'),
    });

    const results1 = await orchestrator1.run('sales');
    
    expect(results1[0].status).toBe('success');
    expect(results1[0].rowsProcessed).toBe(2);

    const facts = await store.getFacts('fact_sales');
    expect(facts).toHaveLength(2);
    expect(facts[0].id).toBe(3);
    expect(facts[1].id).toBe(4);
  });

  it('should apply SCD Type 2 changes correctly', async () => {
    const productsCsv = join(testDir, 'data', 'products.csv');
    
    await writeFile(
      productsCsv,
      'product_id,product_name,category,price,valid_from\n' +
      'P001,Widget A,Electronics,30.00,2024-01-01\n' +
      'P002,Gadget B,Electronics,75.00,2024-01-01'
    );

    const config: ETLConfig = {
      sources: [
        {
          name: 'products_csv',
          type: 'csv',
          connection: { filePath: productsCsv, hasHeader: true },
          schema: 'product',
        },
      ],
      target: { type: 'memory' },
      cubes: [
        {
          name: 'sales',
          factTable: 'fact_sales',
          dimensions: ['product'],
          measures: [],
          sources: ['products_csv'],
        },
      ],
    };

    const store = new InMemoryStore();

    const orchestrator1 = new ETLOrchestrator({
      config,
      store,
      metadataPath: join(testDir, 'metadata'),
    });

    await orchestrator1.run('sales');

    let products = await store.getDimensions('dim_product', { is_current: true });
    expect(products).toHaveLength(2);
    expect(products.find(p => p.product_id === 'P001')?.price).toBe(30.00);

    await writeFile(
      productsCsv,
      'product_id,product_name,category,price,valid_from\n' +
      'P001,Widget A,Electronics,35.00,2024-01-15\n' +
      'P002,Gadget B,Electronics,75.00,2024-01-01'
    );

    const orchestrator2 = new ETLOrchestrator({
      config,
      store,
      metadataPath: join(testDir, 'metadata'),
    });

    await orchestrator2.run('sales');

    products = await store.getDimensions('dim_product', { is_current: true });
    expect(products).toHaveLength(2);
    expect(products.find(p => p.product_id === 'P001')?.price).toBe(35.00);

    const allProducts = await store.getDimensions('dim_product');
    expect(allProducts.filter(p => p.product_id === 'P001')).toHaveLength(2);
    
    const oldVersion = allProducts.find(p => p.product_id === 'P001' && !p.is_current);
    expect(oldVersion?.price).toBe(30.00);
    expect(oldVersion?.valid_to).toBeDefined();
  });
});
