import express from 'express';
import axios from 'axios';
import db from './db.js';
import { connectToBroker, publishMessage } from './broker.js';

const app = express();
app.use(express.json());

// RabbitMQ
connectToBroker().catch(err => console.error('Broker init error', err));

// Create order
app.post('/', async (req, res) => {
  // TODO: Implement order creation with the following steps:
  // 1. Validate request body:
  //    - Check productId exists
  //    - Check quantity is positive
  // 2. Call product service to verify product exists:
  //    - Use axios to GET product details
  //    - Handle timeouts and errors
  // 3. Insert order into database:
  //    - Add to orders table with PENDING status
  // 4. Publish order.created event to message broker:
  //    - Include order id, product details, quantity
  // 5. Return success response with order details
  try {
    // 1. Validate request body
    const { productId, quantity } = req.body;
    if (!productId) {
      return res.status(400).json({ error: 'productId is required' });
    }
    if (!quantity || quantity <= 0) {
      return res.status(400).json({ error: 'quantity must be a positive number' });
    }

    // 2. Call product service to verify product exists
    const productServiceUrl = process.env.PRODUCT_SERVICE_URL || 'http://product-service:8002';
    let product;
    try {
      const response = await axios.get(`${productServiceUrl}/${productId}`, {
        timeout: 5000 // 5 second timeout
      });
      product = response.data;
    } catch (error) {
      if (error.response?.status === 404) {
        return res.status(404).json({ error: 'Product not found' });
      }
      console.error('Error calling product service:', error.message);
      return res.status(503).json({ error: 'Unable to verify product. Service unavailable.' });
    }

    // 3. Insert order into database
    const insertResult = await db.query(
      'INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, $3) RETURNING *',
      [productId, quantity, 'PENDING']
    );
    const order = insertResult.rows[0];

    // 4. Publish order.created event to message broker
    try {
      await publishMessage('orders', {
        event: 'ORDER_CREATED',
        orderId: order.id,
        productId: product.id,
        productName: product.name,
        quantity: quantity,
        status: order.status,
        createdAt: order.created_at
      });
      console.log(`Published ORDER_CREATED event for order ${order.id}`);
    } catch (brokerError) {
      console.error('Failed to publish message to broker:', brokerError.message);
      // Note: We still return success since the order was saved
    }

    // 5. Return success response with order details
    res.status(201).json({
      id: order.id,
      productId: order.product_id,
      productName: product.name,
      quantity: order.quantity,
      status: order.status,
      createdAt: order.created_at
    });
  } catch (error) {
    console.error('Error creating order:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// List orders
app.get('/', async (_req, res) => {
  const r = await db.query('SELECT * FROM orders ORDER BY id DESC');
  res.json(r.rows);
});

// Get order by id
app.get('/:id', async (req, res) => {
  const id = Number(req.params.id);
  const r = await db.query('SELECT * FROM orders WHERE id = $1', [id]);
  if (r.rows.length === 0) return res.status(404).json({ error: 'Order not found' });
  res.json(r.rows[0]);
});

const PORT = 8003;
app.listen(PORT, () => console.log(`Order Service running on ${PORT}`));
