import { z } from 'zod';
import { SCDConfigSchema } from './scd.js';

export const DimensionAttributeSchema = z.object({
  id: z.string(),
  name: z.string(),
  column: z.string(),
  dataType: z.string(),
  description: z.string().optional(),
  isKey: z.boolean().default(false),
});

export const DimensionSchema = z.object({
  id: z.string(),
  name: z.string(),
  tableName: z.string(),
  description: z.string().optional(),
  attributes: z.array(DimensionAttributeSchema).min(1),
  primaryKey: z.string(),
  scdConfig: SCDConfigSchema.optional(),
});

export type DimensionAttribute = z.infer<typeof DimensionAttributeSchema>;
export type Dimension = z.infer<typeof DimensionSchema>;
