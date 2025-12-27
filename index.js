const app = require('./src/app');
const { pool } = require('./src/config/db'); // Import the pool to check connection
require('dotenv').config();

const PORT = process.env.PORT || 3001;

async function startServer() {
  try {
    // 1. Connect the database first
    console.log('Connecting to PostgreSQL database...');
    await pool.query('SELECT 1'); // Simple query to verify connection
    console.log('✅ Database connected successfully');

    // 2. Connect the server after DB is ready
    app.listen(PORT, () => {
      console.log(`🚀 Bharat Explorer Backend running on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error('❌ Database connection failed:');
    console.error(error.message);
    process.exit(1); // Exit if DB connection fails
  }
}

startServer();