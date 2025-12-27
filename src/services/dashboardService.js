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
        SUM(actual_visits)::int as visits,
        SUM(target_visits)::int as target,
        SUM(classroom_obs)::int as obs
      FROM (
        -- 1. HARYANA: DATA + TARGETS (All in one table)
        SELECT 
          udise_code, 
          1 as actual_visits,            -- Each row in survey is 1 actual visit
          0 as target_visits,            -- Targets handled in next block
          1 as classroom_obs,
          'haryana' as state, 
          LOWER(district) as district, 
          LOWER(block) as block 
        FROM surveycto_gsheet_data.haryana_cro_tool_2025_26

        UNION ALL

        -- 2. UP: ACTUAL VISITS & OBS (From Survey Table)
        SELECT 
          udise_code, 
          1 as actual_visits,            -- Each row in survey is 1 actual visit
          0 as target_visits,            -- Targets handled in next block
          1 as classroom_obs,
          'uttar pradesh' as state, 
          LOWER(district) as district, 
          LOWER(block) as block 
        FROM surveycto_gsheet_data.up_cro_tool_2025_2026

        UNION ALL

        -- 3. UP: TARGETS (From Staff Work Plan Table)
        SELECT 
          NULL as udise_code, 
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

      // Get Master School count from your constants
      const geo = GEO_METRICS[displayName.toLowerCase()] || {
        total_schools: 0,
      };

      const targetVal = parseInt(row.target || 0);
      const visitsVal = parseInt(row.visits || 0);

      return {
        name: displayName,
        achievement: targetVal > 0 ? (visitsVal / targetVal) * 100 : 0,
        visits: visitsVal,
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

const getFilteredVisits = async (filters) => {
  const { state, district, block, subject, grade } = filters;

  const haryanaQuery = `
    SELECT
      key as id,
      visit_date,
      ay as academic_year,
      TRIM(TO_CHAR(visit_date, 'Month')) as month,
      (extract(month from visit_date) :: int - 1) as month_index,
      'haryana' as state, -- Standardized small
      district as district,
      block as block,
      (district || '-' || block) as bac_id,
      staff_name as bac_name,
      15 as recommended_visits,
      1 as target_visits,
      1 as actual_visits,
      1 as classroom_obs,
      subject,
      ('Grade ' || class) as grade,
      teacher_gender as gender,
      coalesce(enrolled_students :: int, 0) as students_enrolled,
      coalesce(present_students :: int, 0) as students_present,
      (q3 = 'Yes') as teacher_guide_available,
      CASE WHEN q3 = 'Yes' THEN 'All Steps' WHEN q3 = 'No' THEN 'No Steps' ELSE 'Other' END as teacher_guide_followed,
      CASE WHEN concat_ws('', q3_h_11, q3_m_6) = 'Yes' THEN true ELSE false END AS tracker_filled,
      (class_situation = 'mg') as is_multigrade,
      (ssi_2_effectiveness = 'Yes') as ssi2_effective,
      (ssi_3_effectiveness = 'Yes') as ssi3_effective,
      jsonb_build_object('pp1', ssi_lit_1 = '1', 'pp2', ssi_lit_2 = '1', 'pp3', ssi_lit_3 = '1', 'pp4', ssi_lit_4 = '1', 'gp1', ssi_num_1 = '1', 'gp2', ssi_num_2 = '1', 'gp3', ssi_num_3 = '1') as practices,
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
      'uttar pradesh' as state, -- Standardized small
      lower(district) as district,
      lower(block) as block,
      (district || '-' || block) as bac_id,
      staff_name as bac_name,
      15 as recommended_visits,
      1 as target_visits,
      1 as actual_visits,
      1 as classroom_obs,
      subject,
      ('Grade ' || class) as grade,
      null as gender,
      coalesce(enrolled_students :: int, 0) as students_enrolled,
      coalesce(present_students :: int, 0) as students_present,
      (q3 = 'Yes') as teacher_guide_available,
      CASE WHEN q3 = 'Yes' THEN 'All Steps' WHEN q3 = 'No' THEN 'No Steps' ELSE 'Other' END as teacher_guide_followed,
      CASE WHEN concat_ws('', q3_h_11, q3_m_6) = 'Yes' THEN true ELSE false END AS tracker_filled,
      (class_situation = 'mg') as is_multigrade,
      (ssi_2_effectiveness = 'Yes') as ssi2_effective,
      (ssi_3_effectiveness = 'Yes') as ssi3_effective,
      jsonb_build_object('pp1', ssi_lit_1 = '1', 'pp2', ssi_lit_2 = '1', 'pp3', ssi_lit_3 = '1', 'pp4', ssi_lit_4 = '1', 'gp1', ssi_num_1 = '1', 'gp2', ssi_num_2 = '1', 'gp3', ssi_num_3 = '1') as practices,
      udise_code as school_id,
      username as arp_id
    FROM surveycto_gsheet_data.up_cro_tool_2025_2026
  `;

  let combinedQuery = `SELECT * FROM ((${haryanaQuery}) UNION ALL (${upQuery})) as all_visits WHERE 1=1`;

  const params = [];
  
  if (state && !['All', 'All States'].includes(state)) {
    params.push(state.toLowerCase());
    combinedQuery += ` AND state = $${params.length}`;
  }
  if (district && !['All', 'All Districts'].includes(district)) {
    params.push(district.toLowerCase());
    combinedQuery += ` AND lower(district) = $${params.length}`;
  }
  if (block && !['All', 'All Blocks'].includes(block)) {
    params.push(block.toLowerCase());
    combinedQuery += ` AND lower(block) = $${params.length}`;
  }
  if (subject && subject !== 'All') {
    params.push(subject.toLowerCase());
    combinedQuery += ` AND LOWER(subject) = $${params.length}`;
  }
  if (grade && grade !== 'All') {
    params.push(grade); // Grades are usually specific strings like 'Grade 1'
    combinedQuery += ` AND grade = $${params.length}`;
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

// basic-backend/src/services/dashboardService.js

const wrapGeoJSON = (features) => ({
  type: "FeatureCollection",
  features: features || []
});

// National View: Union districts to create state outlines
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

// State View: Show districts for the selected state
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

// District View: Show sub-districts (blocks) for the selected district
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
    
    // Return standard FeatureCollection format
    return {
      type: "FeatureCollection",
      features: res.rows
    };
  } catch (err) {
    console.error("SQL Error in getBlockBoundaries:", err.message);
    throw err;
  }
};

module.exports = {
  getFilteredVisits,
  getSchoolsByBlock,
  getMetricsByCategory,
  getHierarchyMetrics,
  getNationalBoundaries,
  getStateBoundaries,
  getDistrictBoundaries,
  getBlockBoundaries
};
