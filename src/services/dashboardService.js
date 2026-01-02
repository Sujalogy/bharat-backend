// src/services/dashboardService.js
const db = require("../config/db");

const GEO_METRICS = {
  india: {
    area_sqkm: 3287263,
    population_density: 414,
    total_schools: 1551000,
  },
  haryana: { area_sqkm: 44212, population_density: 573, total_schools: 15300 },
  "uttar pradesh": {
    area_sqkm: 240928,
    population_density: 829,
    total_schools: 258000,
  },
  sirsa: { area_sqkm: 4277, population_density: 303, total_schools: 1100 },
  // ... add the rest here, ensuring keys are lowercase
};

const getHierarchyMetrics = async (filters) => {
  const { state, district, block } = filters;

  const getAggregates = async (lState, lDistrict, lBlock) => {
    const validState =
      lState && lState !== "All" && lState !== "All" ? lState : null;
    const validDistrict =
      lDistrict && lDistrict !== "All" && lDistrict !== "All"
        ? lDistrict
        : null;
    const validBlock =
      lBlock && lBlock !== "All" && lBlock !== "All" ? lBlock : null;

    let query = `
      SELECT 
        COUNT(DISTINCT udise_code) as schools_covered,
        COUNT(DISTINCT visit_date) as unique_visit_days,
        SUM(target_visits)::int as target,
        SUM(classroom_obs)::int as obs
      FROM (
        -- 1. HARYANA: DATA + TARGETS (All in one table)
        SELECT 
          udise_code, 
          visit_date,
          1 as actual_visits,
          0 as target_visits,
          1 as classroom_obs,
          'haryana' as state, 
          LOWER(district) as district, 
          LOWER(block) as block 
        FROM surveycto_gsheet_data.haryana_cro_tool_2025_26

        UNION ALL

        -- 2. UP: ACTUAL VISITS & OBS (From Survey Table)
        SELECT 
          udise_code, 
          visit_date,
          1 as actual_visits,
          0 as target_visits,
          1 as classroom_obs,
          'uttar pradesh' as state, 
          LOWER(district) as district, 
          LOWER(block) as block 
        FROM surveycto_gsheet_data.up_cro_tool_2025_2026

        UNION ALL

        -- 3. UP: TARGETS (From Staff Work Plan Table)
        SELECT 
          NULL as udise_code, 
          NULL as visit_date,
          0 as actual_visits, 
          COALESCE(total_visit_days, 0) as target_visits, 
          0 as classroom_obs, 
          null as state,
          null as district,
          null as block
        FROM master_targets.staff_monthly_work_plan
      ) as all_data
      WHERE 1=1
    `;

    const params = [];
    if (validState) {
      params.push(validState.toLowerCase());
      query += ` AND state = $${params.length}`;
    }
    if (validDistrict) {
      params.push(validDistrict.toLowerCase());
      query += ` AND district = $${params.length}`;
    }
    if (validBlock) {
      params.push(validBlock.toLowerCase());
      query += ` AND block = $${params.length}`;
    }

    try {
      const res = await db.query(query, params);
      const row = res.rows[0];

      const displayName = validBlock || validDistrict || validState || "India";
      const geo = GEO_METRICS[displayName.toLowerCase()] || {
        total_schools: 0,
      };

      const targetVal = parseInt(row.target || 0);
      const uniqueVisitDays = parseInt(row.unique_visit_days || 0);

      return {
        name: displayName,
        achievement: targetVal > 0 ? (uniqueVisitDays / targetVal) * 100 : 0,
        visits: uniqueVisitDays,
        obs: parseInt(row.obs || 0),
        schools_covered: parseInt(row.schools_covered || 0),
        total_schools_master: geo.total_schools,
      };
    } catch (err) {
      console.error("SQL Error in getAggregates:", err.message);
      throw err;
    }
  };

  return {
    national: await getAggregates(null, null, null),
    state:
      state && state !== "All" ? await getAggregates(state, null, null) : null,
    district:
      district && district !== "All" && district !== "All Districts"
        ? await getAggregates(state, district, null)
        : null,
    block:
      block && block !== "All" && block !== "All Blocks"
        ? await getAggregates(state, district, block)
        : null,
  };
};

// UPDATED: getFilteredVisits function
const getFilteredVisits = async (filters) => {
  const { state, district, block, subject, grade, visit_type, month } = filters; // ✅ Add month

  const haryanaQuery = `
    SELECT
      key as id,
      visit_date,
      ay as academic_year,
      TRIM(TO_CHAR(visit_date, 'Month')) as month,
      (extract(month from visit_date) :: int - 1) as month_index,
      'haryana' as state,
      LOWER(district) as district,
      LOWER(block) as block,
      (district || '-' || block) as bac_id,
      staff_name as bac_name,
      15 as recommended_visits,
      
      COALESCE(
        (
          SELECT smwp.total_visit_days
          FROM master_targets.staff_monthly_work_plan smwp
          WHERE LOWER(smwp.staff_name) = LOWER(haryana_cro_tool_2025_26.staff_name)
            AND date_trunc('month', smwp.visit_month_year)
                = date_trunc('month', haryana_cro_tool_2025_26.visit_date)
            AND LOWER(smwp.state) = 'haryana'
          LIMIT 1
        ),
        15
      ) AS target_visits,
      
      1 as actual_visits,
      1 as classroom_obs,
      subject,
      ('Grade ' || class) as grade,
      COALESCE(visit_type, 'Individual') as visit_type,
      teacher_gender as gender,
      coalesce(enrolled_students :: int, 0) as students_enrolled,
      coalesce(present_students :: int, 0) as students_present,
      (q3 = 'Yes') as teacher_guide_available,
      CASE 
        WHEN q3 = 'Yes' THEN 'All Steps' 
        WHEN q3 = 'No' THEN 'No Steps' 
        ELSE 'Other' 
      END as teacher_guide_followed,
      CASE 
        WHEN concat_ws('', q3_h_11, q3_m_6) = 'Yes' THEN true 
        ELSE false 
      END AS tracker_filled,
      (class_situation = 'mg') as is_multigrade,
      (ssi_2_effectiveness = 'Yes') as ssi2_effective,
      (ssi_3_effectiveness = 'Yes') as ssi3_effective,
      jsonb_build_object(
        'pp1', ssi_lit_1 = '1', 
        'pp2', ssi_lit_2 = '1', 
        'pp3', ssi_lit_3 = '1', 
        'pp4', ssi_lit_4 = '1', 
        'gp1', ssi_num_1 = '1', 
        'gp2', ssi_num_2 = '1', 
        'gp3', ssi_num_3 = '1'
      ) as practices,
      udise_code as school_id,
      username as arp_id
    FROM surveycto_gsheet_data.haryana_cro_tool_2025_26
  `;

  const upQuery = `
    SELECT
      key as id,
      visit_date,
      ay as academic_year,
      TRIM(TO_CHAR(visit_date, 'Month')) as month,
      (extract(month from visit_date) :: int - 1) as month_index,
      'uttar pradesh' as state,
      LOWER(district) as district,
      LOWER(block) as block,
      (district || '-' || block) as bac_id,
      staff_name as bac_name,
      15 as recommended_visits,
      
      COALESCE(
        (
          SELECT smwp.total_visit_days
          FROM master_targets.staff_monthly_work_plan smwp
          WHERE LOWER(smwp.staff_name) = LOWER(up_cro_tool_2025_2026.staff_name)
            AND date_trunc('month', smwp.visit_month_year)
                = date_trunc('month', up_cro_tool_2025_2026.visit_date)
            AND LOWER(smwp.state) = 'uttar pradesh'
          LIMIT 1
        ),
        15
      ) AS target_visits,
      
      1 as actual_visits,
      1 as classroom_obs,
      subject,
      ('Grade ' || class) as grade,
      COALESCE(visit_type, 'Individual') as visit_type,
      null as gender,
      coalesce(enrolled_students :: int, 0) as students_enrolled,
      coalesce(present_students :: int, 0) as students_present,
      (q3 = 'Yes') as teacher_guide_available,
      CASE 
        WHEN q3 = 'Yes' THEN 'All Steps' 
        WHEN q3 = 'No' THEN 'No Steps' 
        ELSE 'Other' 
      END as teacher_guide_followed,
      CASE 
        WHEN concat_ws('', q3_h_11, q3_m_6) = 'Yes' THEN true 
        ELSE false 
      END AS tracker_filled,
      (class_situation = 'mg') as is_multigrade,
      (ssi_2_effectiveness = 'Yes') as ssi2_effective,
      (ssi_3_effectiveness = 'Yes') as ssi3_effective,
      jsonb_build_object(
        'pp1', ssi_lit_1 = '1', 
        'pp2', ssi_lit_2 = '1', 
        'pp3', ssi_lit_3 = '1', 
        'pp4', ssi_lit_4 = '1', 
        'gp1', ssi_num_1 = '1', 
        'gp2', ssi_num_2 = '1', 
        'gp3', ssi_num_3 = '1'
      ) as practices,
      udise_code as school_id,
      username as arp_id
    FROM surveycto_gsheet_data.up_cro_tool_2025_2026
  `;

  let combinedQuery = `SELECT * FROM ((${haryanaQuery}) UNION ALL (${upQuery})) as all_visits WHERE 1=1`;

  const params = [];

  if (state && !["All", "All States"].includes(state)) {
    params.push(state.toLowerCase());
    combinedQuery += ` AND state = $${params.length}`;
  }
  if (district && !["All", "All Districts"].includes(district)) {
    params.push(district.toLowerCase());
    combinedQuery += ` AND lower(district) = $${params.length}`;
  }
  if (block && !["All", "All Blocks"].includes(block)) {
    params.push(block.toLowerCase());
    combinedQuery += ` AND lower(block) = $${params.length}`;
  }
  if (subject && subject !== "All") {
    params.push(subject.toLowerCase());
    combinedQuery += ` AND LOWER(subject) = $${params.length}`;
  }
  if (grade && grade !== "All") {
    params.push(grade);
    combinedQuery += ` AND grade = $${params.length}`;
  }
  if (visit_type && visit_type !== "All") {
    params.push(visit_type);
    combinedQuery += ` AND visit_type = $${params.length}`;
  }
  // ✅ NEW: Month filter
  if (month && month !== "All") {
    params.push(month.trim()); // Use TRIM to match database format
    combinedQuery += ` AND TRIM(month) = $${params.length}`;
  }

  const result = await db.query(
    combinedQuery + " ORDER BY visit_date DESC",
    params
  );
  return result.rows;
};


const getSchoolsByBlock = async (blockName) => {
  const query = `
    SELECT DISTINCT 
      udise_code AS id, 
      school AS name, 
      NULLIF(split_part(geo1, ' ', 1), '')::float AS lat, 
      NULLIF(split_part(geo1, ' ', 2), '')::float AS lng, 
      LOWER(block) as block, 
      LOWER(district) as district,
      'Primary' AS category, 
      'visited' AS visit_status
    FROM (
      SELECT udise_code, school, geo1, block, district FROM surveycto_gsheet_data.haryana_cro_tool_2025_26
      UNION ALL
      SELECT udise_code, school, geo1, block, district FROM surveycto_gsheet_data.up_cro_tool_2025_2026
    ) as all_schools
    WHERE LOWER(block) = $1 
    AND geo1 IS NOT NULL AND geo1 != '';
  `;
  const result = await db.query(query, [blockName.toLowerCase()]);
  return result.rows;
};

const getMetricsByCategory = async (category) => {
  const result = await db.query(
    "SELECT data FROM dashboard_metrics WHERE category = $1",
    [category]
  );
  return result.rows[0]?.data || null;
};

const wrapGeoJSON = (features) => ({
  type: "FeatureCollection",
  features: features || [],
});

const getNationalBoundaries = async () => {
  const query = `
    SELECT 
      MIN(ogc_fid) as id, 
      state_ut as st_nm, 
      ST_AsGeoJSON(ST_Union(ST_GeomFromGeoJSON(geojson::jsonb->>'geometry')))::jsonb as geometry,
      jsonb_build_object('st_nm', state_ut) as properties
    FROM district_geojson
    WHERE geojson IS NOT NULL AND geojson != ''
    GROUP BY state_ut;
  `;
  const res = await db.query(query);
  return wrapGeoJSON(res.rows);
};

const getStateBoundaries = async (stateName) => {
  const query = `
    SELECT 
      ogc_fid as id, 
      district as dt_name, 
      (geojson::jsonb->'geometry') as geometry, 
      jsonb_build_object('dt_name', district, 'st_nm', state_ut) as properties
    FROM district_geojson
    WHERE LOWER(state_ut) = $1 AND geojson IS NOT NULL;
  `;
  const res = await db.query(query, [stateName.toLowerCase()]);
  return wrapGeoJSON(res.rows);
};

const getDistrictBoundaries = async (distName) => {
  const query = `
    SELECT 
      ogc_fid as id, 
      sub_dist as ac_name, 
      (geojson::jsonb->'geometry') as geometry, 
      jsonb_build_object('ac_name', sub_dist, 'dt_name', district, 'st_nm', state_ut) as properties
    FROM subdistrict_geojson
    WHERE LOWER(district) = $1 AND geojson IS NOT NULL;
  `;
  const res = await db.query(query, [distName.toLowerCase()]);
  return wrapGeoJSON(res.rows);
};

const getBlockBoundaries = async (blockName) => {
  try {
    const query = `
      SELECT 
        ogc_fid as id, 
        sub_dist as ac_name, 
        (geojson::jsonb->'geometry') as geometry, 
        jsonb_build_object(
          'ac_name', sub_dist, 
          'dt_name', district, 
          'st_nm', state_ut
        ) as properties
      FROM subdistrict_geojson
      WHERE LOWER(sub_dist) = $1 AND geojson IS NOT NULL;
    `;
    const res = await db.query(query, [blockName.toLowerCase()]);

    return {
      type: "FeatureCollection",
      features: res.rows,
    };
  } catch (err) {
    console.error("SQL Error in getBlockBoundaries:", err.message);
    throw err;
  }
};

const getSummaryMetrics = async (filters) => {
  const { state, district, block, year, subject, grade, visit_type, month } = filters;

  // console.log(filters)
  const buildWhereClause = (params) => {
    const conditions = [];
    let paramIndex = 1;
    if (state && state !== 'All') {
      conditions.push(`state = $${paramIndex++}`);
      params.push(state.toLowerCase());
    }
    if (district && district !== 'All') {
      conditions.push(`district = $${paramIndex++}`);
      params.push(district.toLowerCase());
    }
    if (block && block !== 'All') {
      conditions.push(`block = $${paramIndex++}`);
      params.push(block.toLowerCase());
    }
    if (subject && subject !== 'All') {
      conditions.push(`LOWER(subject) = $${paramIndex++}`);
      params.push(subject.toLowerCase());
    }
    if (grade && grade !== 'All') {
      conditions.push(`grade = $${paramIndex++}`);
      params.push(grade);
    }
    if (visit_type && visit_type !== 'All') {
      conditions.push(`visit_type = $${paramIndex++}`);
      params.push(visit_type);
    }
    if (year && year !== 'All') {
      conditions.push(`ay = $${paramIndex++}`);
      params.push(year);
    }
    if (month && month !== 'All') {
      conditions.push(`TRIM(month) = $${paramIndex++}`);
      params.push(month.trim());
    }

    return conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
  };

  const params = [];
  const whereClause = buildWhereClause(params);

  // FIXED: Restructured to avoid nested aggregates
  const summaryQuery = `
    WITH unified_data AS (
      -- Haryana Data
      SELECT
        staff_name as bac_name,
        'haryana' as state,
        LOWER(district) as district,
        LOWER(block) as block,
        visit_date,
        ay,
        TRIM(TO_CHAR(visit_date, 'Month')) as month,
        EXTRACT(MONTH FROM visit_date)::int as month_num,
        subject,
        ('Grade ' || class) as grade,
        COALESCE(visit_type, 'Individual') as visit_type,
        COALESCE(
          (SELECT total_visit_days FROM master_targets.staff_monthly_work_plan 
           WHERE LOWER(staff_name) = LOWER(haryana_cro_tool_2025_26.staff_name)
           AND date_trunc('month', visit_month_year) = date_trunc('month', haryana_cro_tool_2025_26.visit_date)
           AND LOWER(state) = 'haryana' LIMIT 1),
          15
        ) as target_visits,
        15 as recommended_visits
      FROM surveycto_gsheet_data.haryana_cro_tool_2025_26

      UNION ALL

      -- Uttar Pradesh Data
      SELECT
        staff_name as bac_name,
        'uttar pradesh' as state,
        LOWER(district) as district,
        LOWER(block) as block,
        visit_date,
        ay,
        TRIM(TO_CHAR(visit_date, 'Month')) as month,
        EXTRACT(MONTH FROM visit_date)::int as month_num,
        subject,
        ('Grade ' || class) as grade,
        COALESCE(visit_type, 'Individual') as visit_type,
        COALESCE(
          (SELECT total_visit_days FROM master_targets.staff_monthly_work_plan 
           WHERE LOWER(staff_name) = LOWER(up_cro_tool_2025_2026.staff_name)
           AND date_trunc('month', visit_month_year) = date_trunc('month', up_cro_tool_2025_2026.visit_date)
           AND LOWER(state) = 'uttar pradesh' LIMIT 1),
          15
        ) as target_visits,
        15 as recommended_visits
      FROM surveycto_gsheet_data.up_cro_tool_2025_2026
    ),
    
    -- ✅ FIXED: Calculate BAC metrics per month first
    bac_monthly AS (
      SELECT 
        bac_name,
        month,
        COUNT(DISTINCT visit_date) as month_actual_visits,
        MAX(target_visits) as month_target_visits,
        MAX(recommended_visits) as month_recommended_visits,
        CASE 
          WHEN COUNT(DISTINCT visit_date) < MAX(target_visits) THEN 1 
          ELSE 0 
        END as missed_target,
        CASE 
          WHEN MAX(target_visits) < MAX(recommended_visits) THEN 1 
          ELSE 0 
        END as underplanned
      FROM unified_data
      ${whereClause}
      GROUP BY bac_name, month
    ),
    
    -- ✅ FIXED: Aggregate BAC totals
    bac_totals AS (
      SELECT
        bac_name,
        SUM(month_actual_visits) as total_actual,
        SUM(month_target_visits) as total_target,
        SUM(month_recommended_visits) as total_recommended,
        SUM(missed_target) as months_missed,
        SUM(underplanned) as months_underplanned,
        CASE 
          WHEN SUM(month_target_visits) > 0 
          THEN (SUM(month_actual_visits)::float / SUM(month_target_visits) * 100)
          ELSE 0 
        END as achievement_pct,
        CASE 
          WHEN SUM(month_recommended_visits) > 0 
          THEN (SUM(month_target_visits)::float / SUM(month_recommended_visits) * 100)
          ELSE 0 
        END as planning_pct
      FROM bac_monthly
      GROUP BY bac_name
    ),
    
    -- Monthly aggregates
    month_level AS (
      SELECT 
        month,
        month_num,
        COUNT(DISTINCT visit_date) as month_actual,
        SUM(target_visits) as month_target,
        SUM(recommended_visits) as month_recommended
      FROM unified_data
      ${whereClause}
      GROUP BY month, month_num
      ORDER BY month_num
    ),
    
    -- Yearly aggregates
    year_level AS (
      SELECT 
        ay as year,
        COUNT(DISTINCT visit_date) as year_actual,
        SUM(target_visits) as year_target,
        SUM(recommended_visits) as year_recommended
      FROM unified_data
      ${whereClause}
      GROUP BY ay
      ORDER BY ay
    )
    
    SELECT 
      -- Overall Metrics
      (SELECT COUNT(*) FROM bac_totals WHERE total_actual < total_target) as missed_target_bacs,
      (SELECT COUNT(*) FROM bac_totals WHERE total_target != total_recommended) as fluctuated_target_bacs,
      (SELECT SUM(total_actual) FROM bac_totals) as total_actual,
      (SELECT SUM(total_target) FROM bac_totals) as total_target,
      (SELECT SUM(total_recommended) FROM bac_totals) as total_recommended,
      (SELECT COUNT(*) FROM bac_totals WHERE months_missed >= 3) as chronic_underperformers,
      (SELECT COUNT(*) FROM bac_totals WHERE months_underplanned >= 3) as chronic_underplanners,
      (SELECT COUNT(DISTINCT bac_name) FROM unified_data ${whereClause}) as total_bacs,
      
      -- Monthly Aggregates
      (SELECT json_agg(json_build_object(
        'month', month,
        'actual', month_actual,
        'target', month_target,
        'recommended', month_recommended
      ) ORDER BY month_num) FROM month_level) as monthly_data,
      
      -- Yearly Aggregates
      (SELECT json_agg(json_build_object(
        'year', year,
        'actual', year_actual,
        'target', year_target,
        'recommended', year_recommended
      ) ORDER BY year) FROM year_level) as yearly_data,
      
      -- Performance Distribution
      (SELECT COUNT(*) FROM bac_totals WHERE achievement_pct >= 100) as high_performers,
      (SELECT COUNT(*) FROM bac_totals WHERE achievement_pct >= 80 AND achievement_pct < 100) as medium_performers,
      (SELECT COUNT(*) FROM bac_totals WHERE achievement_pct < 80) as low_performers,
      
      -- Planning Distribution
      (SELECT COUNT(*) FROM bac_totals WHERE planning_pct >= 100) as full_planners,
      (SELECT COUNT(*) FROM bac_totals WHERE planning_pct >= 80 AND planning_pct < 100) as partial_planners,
      (SELECT COUNT(*) FROM bac_totals WHERE planning_pct < 80) as under_planners
  `;

  try {
    const result = await db.query(summaryQuery, params);
    const row = result.rows[0];

    const actualAchievement = row.total_target > 0 
      ? (row.total_actual / row.total_target) * 100 
      : 0;
    
    const targetVsPolicy = row.total_recommended > 0 
      ? (row.total_target / row.total_recommended) * 100 
      : 0;

    const avgAchievement = row.total_target > 0
      ? (row.total_actual / row.total_target) * 100
      : 0;

    const avgPlanning = row.total_recommended > 0
      ? (row.total_target / row.total_recommended) * 100
      : 0;

    const totalGap = (row.total_recommended || 0) - (row.total_target || 0);

    return {
      kpis: {
        missedTargetBacs: parseInt(row.missed_target_bacs) || 0,
        fluctuatedTargetBacs: parseInt(row.fluctuated_target_bacs) || 0,
        actualAchievement: parseFloat(actualAchievement.toFixed(1)),
        targetVsPolicy: parseFloat(targetVsPolicy.toFixed(1)),
        chronicUnderperformers: parseInt(row.chronic_underperformers) || 0,
        chronicUnderplanners: parseInt(row.chronic_underplanners) || 0,
        totalBacs: parseInt(row.total_bacs) || 0,
        totalVisits: parseInt(row.total_actual) || 0,
        avgAchievement: parseFloat(avgAchievement.toFixed(1)),
        avgPlanning: parseFloat(avgPlanning.toFixed(1)),
        totalGap: parseInt(totalGap)
      },
      charts: {
        monthly: row.monthly_data || [],
        yearly: row.yearly_data || [],
        performanceDistribution: [
          { name: 'High (≥100%)', value: parseInt(row.high_performers) || 0 },
          { name: 'Medium (80-99%)', value: parseInt(row.medium_performers) || 0 },
          { name: 'Low (<80%)', value: parseInt(row.low_performers) || 0 }
        ],
        planningDistribution: [
          { name: 'Full (≥100%)', value: parseInt(row.full_planners) || 0 },
          { name: 'Partial (80-99%)', value: parseInt(row.partial_planners) || 0 },
          { name: 'Under (<80%)', value: parseInt(row.under_planners) || 0 }
        ]
      },
      meta: {
        totalActual: parseInt(row.total_actual) || 0,
        totalTarget: parseInt(row.total_target) || 0,
        totalRecommended: parseInt(row.total_recommended) || 0
      }
    };
  } catch (error) {
    console.error('Summary metrics error:', error);
    throw error;
  }
};

/**
 * OPTIMIZED: Get chronic performers list (only when needed)
 */
const getChronicPerformers = async (filters, threshold = 3) => {
  const { state, district, block, year, subject, grade, visit_type, month } = filters;
  
  const params = [threshold];
  let paramIndex = 2;
  const conditions = [];

  if (state && state !== 'All') {
    conditions.push(`state = $${paramIndex++}`);
    params.push(state.toLowerCase());
  }
  if (district && district !== 'All') {
    conditions.push(`district = $${paramIndex++}`);
    params.push(district.toLowerCase());
  }
  if (block && block !== 'All') {
    conditions.push(`block = $${paramIndex++}`);
    params.push(block.toLowerCase());
  }

  const whereClause = conditions.length > 0 ? 'AND ' + conditions.join(' AND ') : '';

  const query = `
    WITH unified_data AS (
      SELECT
        staff_name as bac_name,
        'haryana' as state,
        LOWER(district) as district,
        LOWER(block) as block,
        1 as actual_visits,
        visit_date,
        COALESCE((SELECT total_visit_days FROM master_targets.staff_monthly_work_plan 
                  WHERE LOWER(staff_name) = LOWER(haryana_cro_tool_2025_26.staff_name)
                  AND date_trunc('month', visit_month_year) = date_trunc('month', haryana_cro_tool_2025_26.visit_date)
                  AND LOWER(state) = 'haryana' LIMIT 1), 15) as target_visits
      FROM surveycto_gsheet_data.haryana_cro_tool_2025_26
      
      UNION ALL
      
      SELECT
        staff_name as bac_name,
        'uttar pradesh' as state,
        LOWER(district) as district,
        LOWER(block) as block,
        1 as actual_visits,
        visit_date,

        COALESCE((SELECT total_visit_days FROM master_targets.staff_monthly_work_plan 
                  WHERE LOWER(staff_name) = LOWER(up_cro_tool_2025_2026.staff_name)
                  AND date_trunc('month', visit_month_year) = date_trunc('month', up_cro_tool_2025_2026.visit_date)
                  AND LOWER(state) = 'uttar pradesh' LIMIT 1), 15) as target_visits
      FROM surveycto_gsheet_data.up_cro_tool_2025_2026
    ),
    bac_agg AS (
      SELECT 
        bac_name,
        state,
        district,
        block,
        COUNT(DISTINCT visit_date) as total_actual,
        SUM(target_visits) as total_target,
        COUNT(CASE WHEN actual_visits < target_visits THEN 1 END) as months_missed
      FROM unified_data
      WHERE 1=1 ${whereClause}
      GROUP BY bac_name, state, district, block, visit_date
      HAVING COUNT(CASE WHEN actual_visits < target_visits THEN 1 END) >= $1
    )
    SELECT 
      bac_name,
      state,
      district,
      block,
      months_missed,
      total_actual,
      total_target,
      CASE 
        WHEN total_target > 0 THEN ROUND((total_actual::float / total_target * 100)::numeric, 1)
        ELSE 0 
      END as avg_achievement,
      CASE 
        WHEN total_target > 0 AND (total_actual::float / total_target * 100) < 70 THEN 'critical'
        ELSE 'warning'
      END as status
    FROM bac_agg
    ORDER BY months_missed DESC, avg_achievement ASC
    LIMIT 100;
  `;

  const result = await db.query(query, params);
  return result.rows;
};

/**
 * OPTIMIZED: Get chronic planners list (only when needed)
 */
const getChronicPlanners = async (filters, threshold = 3) => {
  const { state, district, block } = filters;
  
  const params = [threshold];
  let paramIndex = 2;
  const conditions = [];

  if (state && state !== 'All') {
    conditions.push(`state = $${paramIndex++}`);
    params.push(state.toLowerCase());
  }
  if (district && district !== 'All') {
    conditions.push(`district = $${paramIndex++}`);
    params.push(district.toLowerCase());
  }
  if (block && block !== 'All') {
    conditions.push(`block = $${paramIndex++}`);
    params.push(block.toLowerCase());
  }

  const whereClause = conditions.length > 0 ? 'AND ' + conditions.join(' AND ') : '';

  const query = `
    WITH unified_data AS (
      SELECT
        staff_name as bac_name,
        'haryana' as state,
        LOWER(district) as district,
        LOWER(block) as block,
        COALESCE((SELECT total_visit_days FROM master_targets.staff_monthly_work_plan 
                  WHERE LOWER(staff_name) = LOWER(haryana_cro_tool_2025_26.staff_name)
                  AND date_trunc('month', visit_month_year) = date_trunc('month', haryana_cro_tool_2025_26.visit_date)
                  AND LOWER(state) = 'haryana' LIMIT 1), 15) as target_visits,
        15 as recommended_visits
      FROM surveycto_gsheet_data.haryana_cro_tool_2025_26
      
      UNION ALL
      
      SELECT
        staff_name as bac_name,
        'uttar pradesh' as state,
        LOWER(district) as district,
        LOWER(block) as block,
        COALESCE((SELECT total_visit_days FROM master_targets.staff_monthly_work_plan 
                  WHERE LOWER(staff_name) = LOWER(up_cro_tool_2025_2026.staff_name)
                  AND date_trunc('month', visit_month_year) = date_trunc('month', up_cro_tool_2025_2026.visit_date)
                  AND LOWER(state) = 'uttar pradesh' LIMIT 1), 15) as target_visits,
        15 as recommended_visits
      FROM surveycto_gsheet_data.up_cro_tool_2025_2026
    ),
    bac_agg AS (
      SELECT 
        bac_name,
        state,
        district,
        block,
        SUM(target_visits) as total_target,
        SUM(recommended_visits) as total_recommended,
        COUNT(CASE WHEN target_visits < recommended_visits THEN 1 END) as months_underplanned
      FROM unified_data
      WHERE 1=1 ${whereClause}
      GROUP BY bac_name, state, district, block
      HAVING COUNT(CASE WHEN target_visits < recommended_visits THEN 1 END) >= $1
    )
    SELECT 
      bac_name,
      state,
      district,
      block,
      months_underplanned,
      total_target,
      total_recommended,
      (total_recommended - total_target) as planning_gap,
      CASE 
        WHEN total_recommended > 0 THEN ROUND((total_target::float / total_recommended * 100)::numeric, 1)
        ELSE 0 
      END as avg_planning,
      CASE 
        WHEN total_recommended > 0 AND (total_target::float / total_recommended * 100) < 70 THEN 'critical'
        ELSE 'warning'
      END as status
    FROM bac_agg
    ORDER BY months_underplanned DESC, avg_planning ASC
    LIMIT 100;
  `;

  const result = await db.query(query, params);
  return result.rows;
};

module.exports = {
  getFilteredVisits,
  getSchoolsByBlock,
  getMetricsByCategory,
  getHierarchyMetrics,
  getNationalBoundaries,
  getStateBoundaries,
  getDistrictBoundaries,
  getBlockBoundaries,
  getSummaryMetrics,
  getChronicPerformers,
  getChronicPlanners,
};
