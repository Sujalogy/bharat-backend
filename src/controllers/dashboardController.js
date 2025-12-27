const dashboardService = require('../services/dashboardService');

const getAnalytics = async (req, res) => {
  try {
    const filters = {
      state: req.query.state,
      district: req.query.district,
      block: req.query.block,
      year: req.query.year,
      subject: req.query.subject, // Capture subject
      grade: req.query.grade      // Capture grade
    };
    const data = await dashboardService.getFilteredVisits(filters);
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
};

const getSchoolsByBlock = async (req, res) => {
  const { block } = req.query;
  try {
    const schools = await dashboardService.getSchoolsByBlock(block);
    res.json(schools);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch schools' });
  }
};

const getDashboardData = async (req, res) => {
  // If called as /api/cro, the route or middleware should determine category
  const category = req.path.split('/').pop() || 'cro'; 
  try {
    const data = await dashboardService.getMetricsByCategory(category);
    res.json(data || {});
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch dashboard data' });
  }
};

const getHierarchy = async (req, res) => {
  try {
    const metrics = await dashboardService.getHierarchyMetrics(req.query);
    res.json(metrics);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch hierarchy' });
  }
};

const getNationalGeo = async (req, res) => {
  try {
    const data = await dashboardService.getNationalBoundaries();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch national boundaries' });
  }
};

const getStateGeo = async (req, res) => {
  try {
    const data = await dashboardService.getStateBoundaries(req.params.stateName);
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch state boundaries' });
  }
};

const getDistrictGeo = async (req, res) => {
  try {
    const data = await dashboardService.getDistrictBoundaries(req.params.distName);
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch district boundaries' });
  }
};

const getBlockGeo = async (req, res) => {
  try {
    const data = await dashboardService.getBlockBoundaries(req.params.blockName);
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch block boundaries' });
  } 
};

module.exports = {
  getAnalytics,
  getSchoolsByBlock,
  getDashboardData,
  getHierarchy,
  getNationalGeo,
  getStateGeo,
  getDistrictGeo,
  getBlockGeo
};