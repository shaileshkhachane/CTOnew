#!/usr/bin/env node

import { Command } from 'commander';
import { readFile } from 'fs/promises';
import { ETLOrchestrator } from '../orchestrator/orchestrator.js';
import { ETLConfig } from '../types.js';

const program = new Command();

program
  .name('etl')
  .description('ETL orchestrator CLI')
  .version('1.0.0');

program
  .command('run')
  .description('Run ETL pipeline for a cube')
  .option('--cube <name>', 'Name of the cube to process')
  .option('--since <date>', 'Process data since this date (ISO format)')
  .option('--config <path>', 'Path to configuration file', './etl-config.json')
  .action(async (options) => {
    try {
      console.log('[CLI] Starting ETL run...');
      console.log(`[CLI] Config: ${options.config}`);
      if (options.cube) {
        console.log(`[CLI] Cube: ${options.cube}`);
      }
      if (options.since) {
        console.log(`[CLI] Since: ${options.since}`);
      }

      const configContent = await readFile(options.config, 'utf-8');
      const config: ETLConfig = JSON.parse(configContent);

      const since = options.since ? new Date(options.since) : undefined;
      if (since && isNaN(since.getTime())) {
        throw new Error('Invalid date format. Use ISO format (e.g., 2024-01-01T00:00:00Z)');
      }

      const orchestrator = new ETLOrchestrator({
        config,
        since,
      });

      const results = await orchestrator.run(options.cube);

      console.log('\n[CLI] ETL run completed');
      console.log('='.repeat(60));
      
      for (const result of results) {
        console.log(`\nCube: ${result.cube}, Source: ${result.source}`);
        console.log(`  Status: ${result.status}`);
        console.log(`  Processed: ${result.rowsProcessed} rows`);
        console.log(`  Inserted: ${result.rowsInserted} rows`);
        console.log(`  Updated: ${result.rowsUpdated} rows`);
        console.log(`  Duration: ${result.endTime.getTime() - result.startTime.getTime()}ms`);
        
        if (result.errors && result.errors.length > 0) {
          console.log(`  Errors:`);
          for (const error of result.errors) {
            console.log(`    - ${error}`);
          }
        }
      }

      const failed = results.filter(r => r.status === 'failed');
      if (failed.length > 0) {
        console.error(`\n[CLI] ${failed.length} source(s) failed`);
        process.exit(1);
      }

      console.log('\n[CLI] All sources processed successfully');
      
      if (options.cube) {
        const store = orchestrator.getStore();
        if ('getStats' in store) {
          console.log('\n[CLI] Storage Statistics:');
          console.log(JSON.stringify((store as any).getStats(), null, 2));
        }
      }

    } catch (error) {
      console.error('[CLI] Error:', error instanceof Error ? error.message : error);
      process.exit(1);
    }
  });

program.parse();
