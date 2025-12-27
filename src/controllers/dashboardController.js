const dashboardService = require('../services/dashboardService');

const getAnalytics = async (req, res) => {
  try {
    const filters = {
      state: req.query.state,
      district: req.query.district,
      block: req.query.block,
      year: req.query.year,
      subject: req.query.subject,
      grade: req.query.grade
    };
    
    const data = await dashboardService.getFilteredVisits(filters);
    
    res.json(data);
  } catch (error) {
    console.error('Analytics error:', error);
    res.status(500).json({ 
      error: 'Internal Server Error',
      message: error.message 
    });
  }
};

const getSchoolsByBlock = async (req, res) => {
  const { block } = req.query;
  
  if (!block) {
    return res.status(400).json({ error: 'Block parameter is required' });
  }
  
  try {
    const schools = await dashboardService.getSchoolsByBlock(block);
    
    res.json(schools);
  } catch (error) {
    console.error('Schools fetch error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch schools',
      message: error.message 
    });
  }
};

const getDashboardData = async (req, res) => {
  const category = req.path.split('/').pop() || 'cro'; 
  
  try {
    const data = await dashboardService.getMetricsByCategory(category);
    
    res.json(data || {});
  } catch (error) {
    console.error('Dashboard data error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch dashboard data',
      message: error.message 
    });
  }
};

const getHierarchy = async (req, res) => {
  try {
    const metrics = await dashboardService.getHierarchyMetrics(req.query);
    
    res.json(metrics);
  } catch (error) {
    console.error('Hierarchy error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch hierarchy',
      message: error.message 
    });
  }
};

const getNationalGeo = async (req, res) => {
  try {
    const data = await dashboardService.getNationalBoundaries();
    
    res.json(data);
  } catch (error) {
    console.error('National geo error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch national boundaries',
      message: error.message 
    });
  }
};

const getStateGeo = async (req, res) => {
  try {
    const { stateName } = req.params;
    
    const data = await dashboardService.getStateBoundaries(stateName);
    res.json(data);
  } catch (error) {
    console.error('State geo error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch state boundaries',
      message: error.message 
    });
  }
};

const getDistrictGeo = async (req, res) => {
  try {
    const { distName } = req.params;
    
    const data = await dashboardService.getDistrictBoundaries(distName);
    res.json(data);
  } catch (error) {
    console.error('District geo error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch district boundaries',
      message: error.message 
    });
  }
};

const getBlockGeo = async (req, res) => {
  try {
    const { blockName } = req.params;
    
    const data = await dashboardService.getBlockBoundaries(blockName);
    res.json(data);
  } catch (error) {
    console.error('Block geo error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch block boundaries',
      message: error.message 
    });
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