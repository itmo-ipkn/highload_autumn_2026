import type { FastifyPluginAsync } from 'fastify';
import { createStore, type ProductInput } from './store.ts';

const body = {
  type: 'object',
  additionalProperties: false,
  required: ['name', 'price'],
  properties: {
    name: { type: 'string', minLength: 1, maxLength: 200 },
    price: { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER },
  },
};

export const productRoutes: FastifyPluginAsync = async (app) => {
  const store = createStore();

  app.addHook('preValidation', async (request) => {
    const value = request.body;
    if (value !== null && typeof value === 'object' && 'name' in value && typeof value.name === 'string') {
      value.name = value.name.trim();
    }
  });

  app.post<{ Body: ProductInput }>('/items', { schema: { body } }, async (request, reply) => {
    return reply.code(201).send(store.create(request.body));
  });

  app.get<{ Params: { id: string } }>('/items/:id', async (request, reply) => {
    const product = store.get(request.params.id);
    return product ?? reply.code(404).send({ message: 'Product not found' });
  });

  app.put<{ Params: { id: string }; Body: ProductInput }>('/items/:id', { schema: { body } }, async (request, reply) => {
    const product = store.replace(request.params.id, request.body);
    return product ?? reply.code(404).send({ message: 'Product not found' });
  });
};
