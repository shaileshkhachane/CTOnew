import { Connector, CSVConnector, PostgresConnector, RESTConnector } from '../connectors/index.js';
import { DataValidator } from '../validation/validator.js';
import { WatermarkTracker } from '../watermark/tracker.js';
import { SCDHandler } from '../scd/handler.js';
import { DataStore, InMemoryStore } from '../storage/store.js';
import { MetadataEmitter } from '../metadata/emitter.js';
import { CubeManager } from '../../cube/manager.js';
import { ETLConfig, DataSource, RefreshMetadata, StagingData } from '../types.js';

export interface OrchestratorOptions {
  config: ETLConfig;
  store?: DataStore;
  cubeManager?: CubeManager;
  metadataPath?: string;
  since?: Date;
}

export class ETLOrchestrator {
  private config: ETLConfig;
  private store: DataStore;
  private cubeManager: CubeManager;
  private validator: DataValidator;
  private watermarkTracker: WatermarkTracker;
  private scdHandler: SCDHandler;
  private metadataEmitter: MetadataEmitter;
  private since?: Date;

  constructor(options: OrchestratorOptions) {
    this.config = options.config;
    this.store = options.store || new InMemoryStore();
    this.cubeManager = options.cubeManager || new CubeManager();
    this.validator = new DataValidator();
    this.watermarkTracker = new WatermarkTracker(options.metadataPath);
    this.scdHandler = new SCDHandler();
    this.metadataEmitter = new MetadataEmitter(options.metadataPath);
    this.since = options.since;
  }

  async run(cubeName?: string): Promise<RefreshMetadata[]> {
    await this.watermarkTracker.load();

    const cubesToProcess = cubeName
      ? this.config.cubes.filter(c => c.name === cubeName)
      : this.config.cubes;

    if (cubesToProcess.length === 0) {
      throw new Error(`Cube '${cubeName}' not found in configuration`);
    }

    const allMetadata: RefreshMetadata[] = [];

    for (const cube of cubesToProcess) {
      console.log(`\n[Orchestrator] Processing cube: ${cube.name}`);

      const sourcesToProcess = this.config.sources.filter(s =>
        cube.sources.includes(s.name)
      );

      for (const source of sourcesToProcess) {
        const metadata = await this.processSource(cube.name, source);
        allMetadata.push(metadata);
        await this.metadataEmitter.emit(metadata);
      }

      if (allMetadata.some(m => m.status === 'success')) {
        this.cubeManager.invalidate(
          cube.name,
          sourcesToProcess.map(s => s.name),
          'ETL refresh completed'
        );
        await this.cubeManager.rebuild(cube.name);
      }
    }

    await this.metadataEmitter.emitSummary(allMetadata);
    await this.watermarkTracker.save();

    return allMetadata;
  }

  private async processSource(cubeName: string, source: DataSource): Promise<RefreshMetadata> {
    const startTime = new Date();
    const metadata: RefreshMetadata = {
      cube: cubeName,
      source: source.name,
      startTime,
      endTime: new Date(),
      rowsProcessed: 0,
      rowsInserted: 0,
      rowsUpdated: 0,
      status: 'success',
    };

    try {
      console.log(`  [Source: ${source.name}] Extracting data...`);
      
      const connector = this.createConnector(source);
      await connector.connect();

      const watermark = this.since || this.watermarkTracker.getWatermark(source.name);
      const staging = await connector.extract(watermark);
      await connector.disconnect();

      metadata.rowsProcessed = staging.data.length;
      console.log(`  [Source: ${source.name}] Extracted ${staging.data.length} rows`);

      if (staging.data.length === 0) {
        console.log(`  [Source: ${source.name}] No new data to process`);
        metadata.endTime = new Date();
        return metadata;
      }

      console.log(`  [Source: ${source.name}] Validating data...`);
      const validationResult = this.validator.validate(staging, {
        uniqueFields: this.getUniqueFields(source.schema),
        requiredFields: this.getRequiredFields(source.schema),
      });

      if (!validationResult.valid) {
        console.error(`  [Source: ${source.name}] Validation failed:`);
        console.error(this.validator.formatErrors(validationResult));
        metadata.status = 'failed';
        metadata.errors = [this.validator.formatErrors(validationResult)];
        metadata.endTime = new Date();
        return metadata;
      }

      console.log(`  [Source: ${source.name}] Validation passed`);

      console.log(`  [Source: ${source.name}] Loading data...`);
      const loadResult = await this.loadData(staging);
      metadata.rowsInserted = loadResult.inserted;
      metadata.rowsUpdated = loadResult.updated;

      if (staging.watermark) {
        this.watermarkTracker.setWatermark(source.name, staging.watermark);
        metadata.watermark = staging.watermark;
      }

      console.log(`  [Source: ${source.name}] Loaded ${loadResult.inserted} new, ${loadResult.updated} updated rows`);
      metadata.endTime = new Date();

    } catch (error) {
      console.error(`  [Source: ${source.name}] Error:`, error);
      metadata.status = 'failed';
      metadata.errors = [error instanceof Error ? error.message : String(error)];
      metadata.endTime = new Date();
    }

    return metadata;
  }

  private createConnector(source: DataSource): Connector {
    switch (source.type) {
      case 'csv':
        return new CSVConnector(
          source.connection as any,
          source.schema,
          source.name,
          source.watermarkColumn
        );
      case 'postgres':
        return new PostgresConnector(
          source.connection as any,
          source.schema,
          source.name,
          source.watermarkColumn
        );
      case 'rest':
        return new RESTConnector(
          source.connection as any,
          source.schema,
          source.name,
          source.watermarkColumn
        );
      default:
        throw new Error(`Unknown connector type: ${source.type}`);
    }
  }

  private async loadData(staging: StagingData): Promise<{ inserted: number; updated: number }> {
    const isFact = staging.schema === 'sales';
    const isDimension = staging.schema === 'product' || staging.schema === 'customer';

    if (isFact) {
      const inserted = await this.store.saveFacts(`fact_${staging.schema}`, staging.data);
      return { inserted, updated: 0 };
    }

    if (isDimension) {
      const tableName = `dim_${staging.schema}`;
      const keyFields = this.getKeyFields(staging.schema);
      const compareFields = this.getCompareFields(staging.schema);

      const existing = await this.store.getDimensions(tableName, { is_current: true });
      const mergeResult = this.scdHandler.mergeType2(
        existing,
        staging.data,
        keyFields,
        compareFields
      );

      const updated = await this.store.updateDimensions(tableName, mergeResult.toUpdate);
      const inserted = await this.store.saveDimensions(tableName, mergeResult.toInsert);

      return { inserted, updated };
    }

    const inserted = await this.store.saveFacts(staging.schema, staging.data);
    return { inserted, updated: 0 };
  }

  private getKeyFields(schema: string): string[] {
    const keyMap: Record<string, string[]> = {
      product: ['product_id'],
      customer: ['customer_id'],
    };
    return keyMap[schema] || ['id'];
  }

  private getCompareFields(schema: string): string[] {
    const compareMap: Record<string, string[]> = {
      product: ['product_name', 'category', 'price'],
      customer: ['customer_name', 'email', 'segment'],
    };
    return compareMap[schema] || [];
  }

  private getUniqueFields(schema: string): string[] {
    const uniqueMap: Record<string, string[]> = {
      sales: ['id'],
      product: ['product_id'],
      customer: ['customer_id'],
    };
    return uniqueMap[schema] || [];
  }

  private getRequiredFields(schema: string): string[] {
    const requiredMap: Record<string, string[]> = {
      sales: ['id', 'product_id', 'customer_id', 'quantity', 'amount'],
      product: ['product_id', 'product_name', 'category'],
      customer: ['customer_id', 'customer_name'],
    };
    return requiredMap[schema] || [];
  }

  getStore(): DataStore {
    return this.store;
  }
}
