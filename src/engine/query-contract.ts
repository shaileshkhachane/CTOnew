import { z } from 'zod';

const scalarValue = z.union([z.string(), z.number()]);

export const axisSchema = z.object({
  dimension: z.string(),
  level: z.string().optional(),
  alias: z.string().optional(),
  sort: z.enum(['asc', 'desc']).optional()
});

const filterOperators = [
  'eq',
  'neq',
  'in',
  'nin',
  'gt',
  'gte',
  'lt',
  'lte',
  'between'
] as const;

export const filterSchema = z.object({
  dimension: z.string(),
  level: z.string().optional(),
  operator: z.enum(filterOperators),
  value: z.union([
    scalarValue,
    z.array(scalarValue).min(1),
    z.tuple([scalarValue, scalarValue])
  ])
});

export const drillSchema = z.object({
  dimension: z.string(),
  fromLevel: z.string(),
  toLevel: z.string(),
  path: z.array(scalarValue).optional()
});

export const rollupSchema = z.object({
  dimension: z.string(),
  level: z.string()
});

export const pivotSchema = z.object({
  rows: z.array(axisSchema).optional(),
  columns: z.array(axisSchema).optional()
});

export const querySchema = z
  .object({
    cube: z.string(),
    mdx: z.string().optional(),
    rows: z.array(axisSchema).optional(),
    columns: z.array(axisSchema).optional(),
    slices: z.array(filterSchema).optional(),
    dices: z.array(filterSchema).optional(),
    filters: z.array(filterSchema).optional(),
    measures: z.array(z.string()).min(1),
    drill: drillSchema.optional(),
    rollup: rollupSchema.optional(),
    pivot: pivotSchema.optional(),
    includeFlattened: z.boolean().optional()
  })
  .strict();

export type AxisSpec = z.infer<typeof axisSchema>;
export type FilterSpec = z.infer<typeof filterSchema>;
export type DrillSpec = z.infer<typeof drillSchema>;
export type RollupSpec = z.infer<typeof rollupSchema>;
export type PivotSpec = z.infer<typeof pivotSchema>;
export type QueryPayload = z.infer<typeof querySchema>;
export type FilterOperator = (typeof filterOperators)[number];
