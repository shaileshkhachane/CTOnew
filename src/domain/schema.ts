import { z } from 'zod';
import { FactTableSchema } from './fact-table.js';
import { DimensionSchema } from './dimension.js';
import { HierarchySchema } from './hierarchy.js';

export const SchemaTypeSchema = z.enum(['STAR', 'SNOWFLAKE']);

export const CubeSchema = z.object({
  id: z.string(),
  name: z.string(),
  description: z.string().optional(),
  schemaType: SchemaTypeSchema,
  factTable: FactTableSchema,
  dimensions: z.array(DimensionSchema).min(1),
  hierarchies: z.array(HierarchySchema).default([]),
});

export type SchemaType = z.infer<typeof SchemaTypeSchema>;
export type Cube = z.infer<typeof CubeSchema>;
