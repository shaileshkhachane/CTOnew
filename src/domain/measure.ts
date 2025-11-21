import { z } from 'zod';

export const AggregationTypeSchema = z.enum(['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'DISTINCT_COUNT']);

export const MeasureSchema = z.object({
  id: z.string(),
  name: z.string(),
  description: z.string().optional(),
  aggregationType: AggregationTypeSchema,
  expression: z.string(),
  format: z.string().optional(),
  isCalculated: z.boolean().default(false),
});

export type Measure = z.infer<typeof MeasureSchema>;
export type AggregationType = z.infer<typeof AggregationTypeSchema>;
