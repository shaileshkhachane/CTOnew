import { Dimension, SCDType2Config } from '../domain/index.js';

export interface SCDVersionRecord {
  [key: string]: unknown;
}

export class SCDResolver {
  resolveCurrentVersion(
    dimension: Dimension,
    records: SCDVersionRecord[]
  ): SCDVersionRecord[] {
    if (!dimension.scdConfig) {
      return records;
    }

    if (dimension.scdConfig.type === 'TYPE_1') {
      return records;
    }

    return this.resolveType2CurrentVersions(dimension, records);
  }

  private resolveType2CurrentVersions(
    dimension: Dimension,
    records: SCDVersionRecord[]
  ): SCDVersionRecord[] {
    const config = dimension.scdConfig as SCDType2Config;
    const currentFlagColumn = config.currentFlagColumn || 'is_current';

    return records.filter(record => {
      const isCurrent = record[currentFlagColumn];
      return isCurrent === true || isCurrent === 1 || isCurrent === '1';
    });
  }

  resolveVersionAtDate(
    dimension: Dimension,
    records: SCDVersionRecord[],
    date: Date
  ): SCDVersionRecord[] {
    if (!dimension.scdConfig || dimension.scdConfig.type === 'TYPE_1') {
      return records;
    }

    const config = dimension.scdConfig as SCDType2Config;
    
    return records.filter(record => {
      const startDate = new Date(record[config.startDateColumn] as string);
      const endDate = record[config.endDateColumn] 
        ? new Date(record[config.endDateColumn] as string)
        : new Date('9999-12-31');

      return date >= startDate && date < endDate;
    });
  }

  mergeVersionHistory(
    dimension: Dimension,
    records: SCDVersionRecord[]
  ): Map<string, SCDVersionRecord[]> {
    if (!dimension.scdConfig || dimension.scdConfig.type === 'TYPE_1') {
      const resultMap = new Map<string, SCDVersionRecord[]>();
      records.forEach(record => {
        const key = String(record[dimension.primaryKey]);
        resultMap.set(key, [record]);
      });
      return resultMap;
    }

    const config = dimension.scdConfig as SCDType2Config;
    const versionMap = new Map<string, SCDVersionRecord[]>();

    records.forEach(record => {
      const naturalKey = this.extractNaturalKey(record, dimension.primaryKey);
      
      if (!versionMap.has(naturalKey)) {
        versionMap.set(naturalKey, []);
      }
      versionMap.get(naturalKey)!.push(record);
    });

    versionMap.forEach((versions) => {
      versions.sort((a, b) => {
        const dateA = new Date(a[config.startDateColumn] as string);
        const dateB = new Date(b[config.startDateColumn] as string);
        return dateA.getTime() - dateB.getTime();
      });
    });

    return versionMap;
  }

  private extractNaturalKey(record: SCDVersionRecord, primaryKey: string): string {
    const keyValue = record[primaryKey];
    if (typeof keyValue === 'string') {
      return keyValue.replace(/-v\d+$/, '');
    }
    return String(keyValue);
  }
}
