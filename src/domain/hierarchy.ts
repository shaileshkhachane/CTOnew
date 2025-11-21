import { z } from 'zod';

export const HierarchyLevelSchema = z.object({
  id: z.string(),
  name: z.string(),
  column: z.string(),
  order: z.number(),
  description: z.string().optional(),
});

export const HierarchySchema = z.object({
  id: z.string(),
  name: z.string(),
  dimensionId: z.string(),
  description: z.string().optional(),
  levels: z.array(HierarchyLevelSchema).min(1),
});

export type HierarchyLevel = z.infer<typeof HierarchyLevelSchema>;
export type Hierarchy = z.infer<typeof HierarchySchema>;
