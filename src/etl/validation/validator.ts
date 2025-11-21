import { ValidationResult, ValidationError, StagingData } from '../types.js';
import { SchemaRegistry } from '../../domain/models.js';

export class DataValidator {
  validate(staging: StagingData, additionalChecks?: {
    uniqueFields?: string[];
    requiredFields?: string[];
  }): ValidationResult {
    const errors: ValidationError[] = [];
    let rowsPassed = 0;
    let rowsFailed = 0;

    const schema = SchemaRegistry[staging.schema as keyof typeof SchemaRegistry];
    if (!schema) {
      throw new Error(`Unknown schema: ${staging.schema}`);
    }

    const seenValues = new Map<string, Set<any>>();
    if (additionalChecks?.uniqueFields) {
      for (const field of additionalChecks.uniqueFields) {
        seenValues.set(field, new Set());
      }
    }

    for (let i = 0; i < staging.data.length; i++) {
      const row = staging.data[i];
      let rowValid = true;

      const result = schema.safeParse(row);
      if (!result.success) {
        rowValid = false;
        for (const issue of result.error.issues) {
          errors.push({
            row: i,
            field: issue.path.join('.'),
            message: issue.message,
            value: issue.path.length > 0 ? row[issue.path[0]] : row,
          });
        }
      }

      if (additionalChecks?.requiredFields) {
        for (const field of additionalChecks.requiredFields) {
          if (row[field] === null || row[field] === undefined || row[field] === '') {
            rowValid = false;
            errors.push({
              row: i,
              field,
              message: `Required field is missing or empty`,
              value: row[field],
            });
          }
        }
      }

      if (additionalChecks?.uniqueFields) {
        for (const field of additionalChecks.uniqueFields) {
          const value = row[field];
          const seen = seenValues.get(field)!;
          
          if (seen.has(value)) {
            rowValid = false;
            errors.push({
              row: i,
              field,
              message: `Duplicate value found`,
              value,
            });
          } else {
            seen.add(value);
          }
        }
      }

      if (rowValid) {
        rowsPassed++;
      } else {
        rowsFailed++;
      }
    }

    return {
      valid: errors.length === 0,
      errors,
      rowsPassed,
      rowsFailed,
    };
  }

  formatErrors(validationResult: ValidationResult): string {
    if (validationResult.valid) {
      return 'All rows passed validation';
    }

    const lines = [
      `Validation failed: ${validationResult.rowsFailed} rows failed, ${validationResult.rowsPassed} rows passed`,
      '',
      'Errors:',
    ];

    const errorsByRow = new Map<number, ValidationError[]>();
    for (const error of validationResult.errors) {
      if (!errorsByRow.has(error.row)) {
        errorsByRow.set(error.row, []);
      }
      errorsByRow.get(error.row)!.push(error);
    }

    for (const [row, rowErrors] of errorsByRow.entries()) {
      lines.push(`  Row ${row}:`);
      for (const error of rowErrors) {
        lines.push(`    - ${error.field}: ${error.message} (value: ${JSON.stringify(error.value)})`);
      }
    }

    return lines.join('\n');
  }
}
