import { z } from 'zod';

export const SCDType = z.enum(['TYPE_1', 'TYPE_2']);

export const SCDType1ConfigSchema = z.object({
  type: z.literal('TYPE_1'),
  overwriteAttributes: z.array(z.string()),
});

export const SCDType2ConfigSchema = z.object({
  type: z.literal('TYPE_2'),
  versionColumn: z.string(),
  startDateColumn: z.string(),
  endDateColumn: z.string(),
  currentFlagColumn: z.string().optional(),
  trackedAttributes: z.array(z.string()),
});

export const SCDConfigSchema = z.discriminatedUnion('type', [
  SCDType1ConfigSchema,
  SCDType2ConfigSchema,
]);

export type SCDType1Config = z.infer<typeof SCDType1ConfigSchema>;
export type SCDType2Config = z.infer<typeof SCDType2ConfigSchema>;
export type SCDConfig = z.infer<typeof SCDConfigSchema>;
