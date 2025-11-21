export interface SCDRecord {
  [key: string]: any;
  valid_from: Date;
  valid_to?: Date | null;
  is_current: boolean;
}

export class SCDHandler {
  mergeType2<T extends SCDRecord>(
    existing: T[],
    incoming: Partial<T>[],
    keyFields: string[],
    compareFields: string[]
  ): { toInsert: T[]; toUpdate: T[] } {
    const toInsert: T[] = [];
    const toUpdate: T[] = [];
    const now = new Date();

    const existingMap = new Map<string, T>();
    for (const record of existing) {
      if (record.is_current) {
        const key = this.buildKey(record, keyFields);
        existingMap.set(key, record);
      }
    }

    for (const incomingRecord of incoming) {
      const key = this.buildKey(incomingRecord, keyFields);
      const existingRecord = existingMap.get(key);

      if (!existingRecord) {
        toInsert.push({
          ...incomingRecord,
          valid_from: now,
          valid_to: null,
          is_current: true,
        } as T);
      } else {
        const hasChanged = this.hasChanges(existingRecord, incomingRecord, compareFields);
        
        if (hasChanged) {
          const updatedExisting = {
            ...existingRecord,
            valid_to: now,
            is_current: false,
          };
          toUpdate.push(updatedExisting);

          const newVersion = {
            ...incomingRecord,
            valid_from: now,
            valid_to: null,
            is_current: true,
          } as T;
          toInsert.push(newVersion);
        }
      }
    }

    return { toInsert, toUpdate };
  }

  private buildKey(record: Partial<any>, keyFields: string[]): string {
    return keyFields.map(field => String(record[field] ?? '')).join('|');
  }

  private hasChanges(existing: any, incoming: Partial<any>, compareFields: string[]): boolean {
    for (const field of compareFields) {
      const existingValue = existing[field];
      const incomingValue = incoming[field];
      
      if (existingValue instanceof Date && incomingValue instanceof Date) {
        if (existingValue.getTime() !== incomingValue.getTime()) {
          return true;
        }
      } else if (existingValue !== incomingValue) {
        return true;
      }
    }
    return false;
  }
}
