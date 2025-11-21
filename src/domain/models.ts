import { z } from 'zod';

export const SalesRecordSchema = z.object({
  id: z.string().or(z.number()).transform(String),
  product_id: z.string().or(z.number()).transform(String),
  customer_id: z.string().or(z.number()).transform(String),
  quantity: z.number().positive(),
  amount: z.number().positive(),
  sale_date: z.coerce.date(),
  region: z.string().min(1),
  updated_at: z.coerce.date().optional(),
});

export const ProductDimensionSchema = z.object({
  product_id: z.string().or(z.number()).transform(String),
  product_name: z.string().min(1),
  category: z.string().min(1),
  price: z.number().positive(),
  valid_from: z.coerce.date(),
  valid_to: z.coerce.date().nullable().optional(),
  is_current: z.boolean().default(true),
});

export const CustomerDimensionSchema = z.object({
  customer_id: z.string().or(z.number()).transform(String),
  customer_name: z.string().min(1),
  email: z.string().email().optional(),
  segment: z.string().min(1),
  valid_from: z.coerce.date(),
  valid_to: z.coerce.date().nullable().optional(),
  is_current: z.boolean().default(true),
});

export type SalesRecord = z.infer<typeof SalesRecordSchema>;
export type ProductDimension = z.infer<typeof ProductDimensionSchema>;
export type CustomerDimension = z.infer<typeof CustomerDimensionSchema>;

export const SchemaRegistry = {
  sales: SalesRecordSchema,
  product: ProductDimensionSchema,
  customer: CustomerDimensionSchema,
};

export type SchemaType = keyof typeof SchemaRegistry;
