const express = require('express');
const router = express.Router();
const dashboardController = require('../controllers/dashboardController');

// 1. GET /api/v1/dashboard/analytics?year=...&state=...
router.get('/v1/dashboard/analytics', dashboardController.getAnalytics);

// 2. GET /schools?block=...
router.get('/schools', dashboardController.getSchoolsByBlock);
router.get('/v1/dashboard/hierarchy', dashboardController.getHierarchy);

// 3. GET /cro
router.get('/cro', dashboardController.getDashboardData); // Reusing category logic

router.get('/geo/national', dashboardController.getNationalGeo);
router.get('/geo/state/:stateName', dashboardController.getStateGeo);
router.get('/geo/district/:distName', dashboardController.getDistrictGeo);
router.get('/geo/block/:blockName', dashboardController.getBlockGeo);

module.exports = router;