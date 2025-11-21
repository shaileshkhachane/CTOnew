import { SCDRecord } from '../scd/handler.js';

export interface DataStore {
  saveFacts(tableName: string, records: Record<string, any>[]): Promise<number>;
  saveDimensions(tableName: string, records: SCDRecord[]): Promise<number>;
  updateDimensions(tableName: string, records: SCDRecord[]): Promise<number>;
  getDimensions(tableName: string, filters?: Record<string, any>): Promise<SCDRecord[]>;
  getFacts(tableName: string, filters?: Record<string, any>): Promise<Record<string, any>[]>;
  clear(tableName: string): Promise<void>;
}

export class InMemoryStore implements DataStore {
  private facts: Map<string, Record<string, any>[]> = new Map();
  private dimensions: Map<string, SCDRecord[]> = new Map();

  async saveFacts(tableName: string, records: Record<string, any>[]): Promise<number> {
    if (!this.facts.has(tableName)) {
      this.facts.set(tableName, []);
    }
    
    const table = this.facts.get(tableName)!;
    table.push(...records);
    return records.length;
  }

  async saveDimensions(tableName: string, records: SCDRecord[]): Promise<number> {
    if (!this.dimensions.has(tableName)) {
      this.dimensions.set(tableName, []);
    }
    
    const table = this.dimensions.get(tableName)!;
    table.push(...records);
    return records.length;
  }

  async updateDimensions(tableName: string, records: SCDRecord[]): Promise<number> {
    if (!this.dimensions.has(tableName)) {
      return 0;
    }

    const table = this.dimensions.get(tableName)!;
    let updated = 0;

    for (const record of records) {
      const index = table.findIndex((r) => {
        return Object.keys(record).every((key) => {
          if (key === 'valid_to' || key === 'is_current') {
            return true;
          }
          return r[key] === record[key];
        });
      });

      if (index !== -1) {
        table[index] = { ...table[index], ...record };
        updated++;
      }
    }

    return updated;
  }

  async getDimensions(tableName: string, filters?: Record<string, any>): Promise<SCDRecord[]> {
    const table = this.dimensions.get(tableName) || [];
    
    if (!filters) {
      return [...table];
    }

    return table.filter((record) => {
      return Object.entries(filters).every(([key, value]) => {
        return record[key] === value;
      });
    });
  }

  async getFacts(tableName: string, filters?: Record<string, any>): Promise<Record<string, any>[]> {
    const table = this.facts.get(tableName) || [];
    
    if (!filters) {
      return [...table];
    }

    return table.filter((record) => {
      return Object.entries(filters).every(([key, value]) => {
        return record[key] === value;
      });
    });
  }

  async clear(tableName: string): Promise<void> {
    this.facts.delete(tableName);
    this.dimensions.delete(tableName);
  }

  getStats() {
    const stats: Record<string, any> = {
      facts: {},
      dimensions: {},
    };

    for (const [table, records] of this.facts.entries()) {
      stats.facts[table] = records.length;
    }

    for (const [table, records] of this.dimensions.entries()) {
      stats.dimensions[table] = records.length;
    }

    return stats;
  }
}
