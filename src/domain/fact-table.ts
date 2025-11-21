import { z } from 'zod';
import { MeasureSchema } from './measure.js';

export const DimensionReferenceSchema = z.object({
  dimensionId: z.string(),
  foreignKey: z.string(),
  role: z.string().optional(),
});

export const FactTableSchema = z.object({
  id: z.string(),
  name: z.string(),
  tableName: z.string(),
  description: z.string().optional(),
  measures: z.array(MeasureSchema).min(1),
  dimensionReferences: z.array(DimensionReferenceSchema).min(1),
  grain: z.string().optional(),
});

export type DimensionReference = z.infer<typeof DimensionReferenceSchema>;
export type FactTable = z.infer<typeof FactTableSchema>;
