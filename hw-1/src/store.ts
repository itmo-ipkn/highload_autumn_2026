import { randomUUID } from 'node:crypto';

export interface ProductInput {
  name: string;
  price: number;
}

export interface Product extends ProductInput {
  id: string;
}

export function createStore() {
  const products = new Map<string, Product>();
  return {
    create(input: ProductInput): Product {
      const product = { id: randomUUID(), ...input };
      products.set(product.id, product);
      return { ...product };
    },
    get(id: string): Product | undefined {
      const product = products.get(id);
      return product && { ...product };
    },
    replace(id: string, input: ProductInput): Product | undefined {
      if (!products.has(id)) return undefined;
      const product = { id, ...input };
      products.set(id, product);
      return { ...product };
    },
  };
}
