const express = require('express');
const router = express.Router();
const dashboardController = require('../controllers/dashboardController');

// CRITICAL FIX: Remove duplicate '/v1/dashboard' from routes
// The base path is already set in app.js as '/api/v1/dashboard'

// 1. Analytics endpoint
router.get('/analytics', dashboardController.getAnalytics);

// 2. Schools endpoint
router.get('/schools', dashboardController.getSchoolsByBlock);

// 3. Hierarchy metrics endpoint
router.get('/hierarchy', dashboardController.getHierarchy);

// 4. CRO dashboard data
router.get('/cro', dashboardController.getDashboardData);

// 5. GeoJSON endpoints
router.get('/geo/national', dashboardController.getNationalGeo);
router.get('/geo/state/:stateName', dashboardController.getStateGeo);
router.get('/geo/district/:distName', dashboardController.getDistrictGeo);
router.get('/geo/block/:blockName', dashboardController.getBlockGeo);

module.exports = router;