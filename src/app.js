const express = require('express');
const cors = require('cors');
const dashboardRoutes = require('./routes/dashboardRoutes');

const app = express();

// Professional Middleware Stack
app.use(cors());
app.use(express.json());

// API Routes
app.use('/api/v1/dashboard', dashboardRoutes);

// Professional Global Error Handler
app.use((err, req, res, next) => {
  const statusCode = err.statusCode || 500;
  const message = err.message || 'Internal Server Error';
  
  console.error(`[API ERROR] ${req.method} ${req.url}:`, err.stack);

  res.status(statusCode).json({
    success: false,
    timestamp: new Date().toISOString(),
    error: {
      message,
      code: statusCode,
      path: req.originalUrl
    }
  });
});

module.exports = app;