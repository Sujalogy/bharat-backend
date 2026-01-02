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
};

const getHierarchyMetrics = async (filters) => {
  const { state, district, block } = filters;

  const getAggregates = async (lState, lDistrict, lBlock) => {
    const validState = lState && lState !== "All" ? lState : null;
    const validDistrict = lDistrict && lDistrict !== "All" ? lDistrict : null;
    const validBlock = lBlock && lBlock !== "All" ? lBlock : null;

    // ✅ OPTIMIZED: Single unified CTE with all data, then aggregate
    let query = `
      WITH unified_visits AS (
        -- Haryana visits
        SELECT 
          udise_code, 
          visit_date,
          'haryana' as state, 
          LOWER(district) as district, 
          LOWER(block) as block 
        FROM surveycto_gsheet_data.haryana_cro_tool_2025_26
        
        UNION ALL
        
        -- UP visits
        SELECT 
          udise_code, 
          visit_date,
          'uttar pradesh' as state, 
          LOWER(district) as district, 
          LOWER(block) as block 
        FROM surveycto_gsheet_data.up_cro_tool_2025_2026
      ),
      unified_targets AS (
        -- All targets from work plan
        SELECT 
          LOWER(state) as state,
          LOWER(district) as district,
          LOWER(block) as block,
          SUM(COALESCE(total_visit_days, 0)) as total_target
        FROM master_targets.staff_monthly_work_plan
        GROUP BY LOWER(state), LOWER(district), LOWER(block)
      )
      SELECT 
        COUNT(DISTINCT v.udise_code) as schools_covered,
        COUNT(DISTINCT v.visit_date) as unique_visit_days,
        COALESCE(t.total_target, 0) as target,
        COUNT(*) as obs
      FROM unified_visits v
      LEFT JOIN unified_targets t 
        ON v.state = t.state 
        AND v.district = t.district 
        AND v.block = t.block
      WHERE 1=1
    `;

    const params = [];
    if (validState) {
      params.push(validState.toLowerCase());
      query += ` AND v.state = $${params.length}`;
    }
    if (validDistrict) {
      params.push(validDistrict.toLowerCase());
      query += ` AND v.district = $${params.length}`;
    }
    if (validBlock) {
      params.push(validBlock.toLowerCase());
      query += ` AND v.block = $${params.length}`;
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
      district && district !== "All"
        ? await getAggregates(state, district, null)
        : null,
    block:
      block && block !== "All"
        ? await getAggregates(state, district, block)
        : null,
  };
};

// UPDATED: getFilteredVisits function
const getFilteredVisits = async (filters) => {
  const { state, district, block, subject, grade, visit_type, month } = filters;

  const query = `
    WITH all_visits AS (
      -- Haryana data
      SELECT
        h.key as id,
        h.visit_date,
        h.ay as academic_year,
        TRIM(TO_CHAR(h.visit_date, 'Month')) as month,
        (EXTRACT(MONTH FROM h.visit_date)::int - 1) as month_index,
        'haryana' as state,
        LOWER(h.district) as district,
        LOWER(h.block) as block,
        (h.district || '-' || h.block) as bac_id,
        h.staff_name as bac_name,
        15 as recommended_visits,
        COALESCE(t.total_visit_days, 15) AS target_visits,
        1 as actual_visits,
        1 as classroom_obs,
        h.subject,
        ('Grade ' || h.class) as grade,
        COALESCE(h.visit_type, 'Individual') as visit_type,
        h.teacher_gender as gender,
        COALESCE(h.enrolled_students::int, 0) as students_enrolled,
        COALESCE(h.present_students::int, 0) as students_present,
        (h.q3 = 'Yes') as teacher_guide_available,
        CASE 
          WHEN h.q3 = 'Yes' THEN 'All Steps' 
          WHEN h.q3 = 'No' THEN 'No Steps' 
          ELSE 'Other' 
        END as teacher_guide_followed,
        CASE 
          WHEN CONCAT_WS('', h.q3_h_11, h.q3_m_6) = 'Yes' THEN true 
          ELSE false 
        END AS tracker_filled,
        (h.class_situation = 'mg') as is_multigrade,
        (h.ssi_2_effectiveness = 'Yes') as ssi2_effective,
        (h.ssi_3_effectiveness = 'Yes') as ssi3_effective,
        json_build_object(
            'pp_lit_1', ROUND(COUNT(*) FILTER (WHERE h.ssi_lit_1 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Hindi') OVER (), 0) * 100, 1),
            'pp_lit_2', ROUND(COUNT(*) FILTER (WHERE h.ssi_lit_2 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Hindi') OVER (), 0) * 100, 1),
            'pp_lit_3', ROUND(COUNT(*) FILTER (WHERE h.ssi_lit_3 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Hindi') OVER (), 0) * 100, 1),
            'pp_lit_4', ROUND(COUNT(*) FILTER (WHERE h.ssi_lit_4 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Hindi') OVER (), 0) * 100, 1),
            'pp_num_1', ROUND(COUNT(*) FILTER (WHERE h.ssi_num_1 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Math') OVER (), 0) * 100, 1),
            'pp_num_2', ROUND(COUNT(*) FILTER (WHERE h.ssi_num_2 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Math') OVER (), 0) * 100, 1),
            'pp_num_3', ROUND(COUNT(*) FILTER (WHERE h.ssi_num_3 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Math') OVER (), 0) * 100, 1),
            'pp_num_4', ROUND(COUNT(*) FILTER (WHERE h.ssi_num_4 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Math') OVER (), 0) * 100, 1),
            'gp_lit_1', ROUND(COUNT(*) FILTER (WHERE h.subject = 'Hindi' AND h.q4_4 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Hindi') OVER (), 0) * 100, 1),
            'gp_lit_2', ROUND(COUNT(*) FILTER (WHERE h.subject = 'Hindi' AND h.q4_8 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Hindi') OVER (), 0) * 100, 1),
            'gp_lit_3', ROUND(COUNT(*) FILTER (WHERE h.subject = 'Hindi' AND h.q3_h_7 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Hindi') OVER (), 0) * 100, 1),
            'gp_num_1', ROUND(COUNT(*) FILTER (WHERE h.subject = 'Math' AND h.q4_4 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Math') OVER (), 0) * 100, 1),
            'gp_num_2', ROUND(COUNT(*) FILTER (WHERE h.subject = 'Math' AND h.q4_8 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Math') OVER (), 0) * 100, 1),
            'gp_num_3', ROUND(COUNT(*) FILTER (WHERE h.subject = 'Math' AND h.q3_h_7 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE h.subject = 'Math') OVER (), 0) * 100, 1)
        ) AS practice_indicators,
        h.udise_code as school_id,
        h.username as arp_id
      FROM surveycto_gsheet_data.haryana_cro_tool_2025_26 h
      LEFT JOIN master_targets.staff_monthly_work_plan t
        ON LOWER(t.staff_name) = LOWER(h.staff_name)
        AND DATE_TRUNC('month', t.visit_month_year) = DATE_TRUNC('month', h.visit_date)
        AND LOWER(t.state) = 'haryana'
      
      UNION ALL
      
      -- UP data
      SELECT
        u.key as id,
        u.visit_date,
        u.ay as academic_year,
        TRIM(TO_CHAR(u.visit_date, 'Month')) as month,
        (EXTRACT(MONTH FROM u.visit_date)::int - 1) as month_index,
        'uttar pradesh' as state,
        LOWER(u.district) as district,
        LOWER(u.block) as block,
        (u.district || '-' || u.block) as bac_id,
        u.staff_name as bac_name,
        15 as recommended_visits,
        COALESCE(t.total_visit_days, 15) AS target_visits,
        1 as actual_visits,
        1 as classroom_obs,
        u.subject,
        ('Grade ' || u.class) as grade,
        COALESCE(u.visit_type, 'Individual') as visit_type,
        null as gender,
        COALESCE(u.enrolled_students::int, 0) as students_enrolled,
        COALESCE(u.present_students::int, 0) as students_present,
        (u.q3 = 'Yes') as teacher_guide_available,
        CASE 
          WHEN u.q3 = 'Yes' THEN 'All Steps' 
          WHEN u.q3 = 'No' THEN 'No Steps' 
          ELSE 'Other' 
        END as teacher_guide_followed,
        CASE 
          WHEN CONCAT_WS('', u.q3_h_11, u.q3_m_6) = 'Yes' THEN true 
          ELSE false 
        END AS tracker_filled,
        (u.class_situation = 'mg') as is_multigrade,
        (u.ssi_2_effectiveness = 'Yes') as ssi2_effective,
        (u.ssi_3_effectiveness = 'Yes') as ssi3_effective,
        json_build_object(
            'pp_lit_1', ROUND(COUNT(*) FILTER (WHERE u.ssi_lit_1 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Hindi') OVER (), 0) * 100, 1),
            'pp_lit_2', ROUND(COUNT(*) FILTER (WHERE u.ssi_lit_2 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Hindi') OVER (), 0) * 100, 1),
            'pp_lit_3', ROUND(COUNT(*) FILTER (WHERE u.ssi_lit_3 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Hindi') OVER (), 0) * 100, 1),
            'pp_lit_4', ROUND(COUNT(*) FILTER (WHERE u.ssi_lit_4 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Hindi') OVER (), 0) * 100, 1),
            'pp_num_1', ROUND(COUNT(*) FILTER (WHERE u.ssi_num_1 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Math') OVER (), 0) * 100, 1),
            'pp_num_2', ROUND(COUNT(*) FILTER (WHERE u.ssi_num_2 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Math') OVER (), 0) * 100, 1),
            'pp_num_3', ROUND(COUNT(*) FILTER (WHERE u.ssi_num_3 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Math') OVER (), 0) * 100, 1),
            'pp_num_4', ROUND(COUNT(*) FILTER (WHERE u.ssi_num_4 = '1') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Math') OVER (), 0) * 100, 1),
            'gp_lit_1', ROUND(COUNT(*) FILTER (WHERE u.subject = 'Hindi' AND u.q4_4 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Hindi') OVER (), 0) * 100, 1),
            'gp_lit_2', ROUND(COUNT(*) FILTER (WHERE u.subject = 'Hindi' AND u.q4_8 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Hindi') OVER (), 0) * 100, 1),
            'gp_lit_3', ROUND(COUNT(*) FILTER (WHERE u.subject = 'Hindi' AND u.q3_h_7 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Hindi') OVER (), 0) * 100, 1),
            'gp_num_1', ROUND(COUNT(*) FILTER (WHERE u.subject = 'Math' AND u.q4_4 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Math') OVER (), 0) * 100, 1),
            'gp_num_2', ROUND(COUNT(*) FILTER (WHERE u.subject = 'Math' AND u.q4_8 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Math') OVER (), 0) * 100, 1),
            'gp_num_3', ROUND(COUNT(*) FILTER (WHERE u.subject = 'Math' AND u.q3_h_7 = 'Yes') OVER ()::numeric / NULLIF(COUNT(*) FILTER (WHERE u.subject = 'Math') OVER (), 0) * 100, 1)
        ) AS practice_indicators,
        u.udise_code as school_id,
        u.username as arp_id
      FROM surveycto_gsheet_data.up_cro_tool_2025_2026 u
      LEFT JOIN master_targets.staff_monthly_work_plan t
        ON LOWER(t.staff_name) = LOWER(u.staff_name)
        AND DATE_TRUNC('month', t.visit_month_year) = DATE_TRUNC('month', u.visit_date)
        AND LOWER(t.state) = 'uttar pradesh'
    )
    SELECT * FROM all_visits WHERE 1=1
  `;

  const params = [];
  let whereClause = "";

  if (state && !["All", "All States"].includes(state)) {
    params.push(state.toLowerCase());
    whereClause += ` AND state = $${params.length}`;
  }
  if (district && !["All", "All Districts"].includes(district)) {
    params.push(district.toLowerCase());
    whereClause += ` AND district = $${params.length}`;
  }
  if (block && !["All", "All Blocks"].includes(block)) {
    params.push(block.toLowerCase());
    whereClause += ` AND block = $${params.length}`;
  }
  if (subject && subject !== "All") {
    params.push(subject.toLowerCase());
    whereClause += ` AND subject = $${params.length}`;
  }
  if (grade && grade !== "All") {
    params.push(grade);
    whereClause += ` AND grade = $${params.length}`;
  }
  if (visit_type && visit_type !== "All") {
    params.push(visit_type);
    whereClause += ` AND visit_type = $${params.length}`;
  }
  if (month && month !== "All") {
    params.push(month.trim());
    whereClause += ` AND TRIM(month) = $${params.length}`;
  }

  const result = await db.query(
    query + whereClause + " ORDER BY visit_date DESC",
    params
  );
  return result.rows;
};

const getSchoolsByBlock = async (blockName) => {
  const query = `
    SELECT DISTINCT 
      udise_code AS id, 
      school AS name, 
      NULLIF(SPLIT_PART(geo1, ' ', 1), '')::float AS lat, 
      NULLIF(SPLIT_PART(geo1, ' ', 2), '')::float AS lng, 
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

/**
 * GET COMPREHENSIVE CRO METRICS
 * Aggregates all CRO-related data from both Haryana and UP
 */
const getCROMetrics = async (filters) => {
  const { state, district, block, year, month, subject, grade, visit_type } =
    filters;

  const params = [];
  let paramIndex = 1;
  const conditions = [];

  // Build WHERE clause dynamically
  if (state && state !== "All") {
    conditions.push(`state = $${paramIndex++}`);
    params.push(state.toLowerCase());
  }
  if (district && district !== "All") {
    conditions.push(`district = $${paramIndex++}`);
    params.push(district.toLowerCase());
  }
  if (block && block !== "All") {
    conditions.push(`block = $${paramIndex++}`);
    params.push(block.toLowerCase());
  }
  if (year && year !== "All") {
    conditions.push(`ay = $${paramIndex++}`);
    params.push(year);
  }
  if (month && month !== "All") {
    conditions.push(`TRIM(month) = $${paramIndex++}`);
    params.push(month.trim());
  }
  if (subject && subject !== "All") {
    conditions.push(`LOWER(subject) = $${paramIndex++}`);
    params.push(subject.toLowerCase());
  }
  if (grade && grade !== "All") {
    conditions.push(`grade = $${paramIndex++}`);
    params.push(grade);
  }
  if (visit_type && visit_type !== "All") {
    conditions.push(`visit_type = $${paramIndex++}`);
    params.push(visit_type);
  }

  const whereClause =
    conditions.length > 0 ? "WHERE " + conditions.join(" AND ") : "";

  const query = `
   WITH unified_cro AS (
    -- Haryana CRO Data
    SELECT 'haryana'                                 as state,
           LOWER(district)                           as district,
           LOWER(block)                              as block,
           ay,
           TRIM(TO_CHAR(visit_date, 'Month'))        as month,
           subject,
           ('Grade ' || class)                       as grade,
           COALESCE(visit_type, 'Individual')        as visit_type,

           -- SSI Indicators
           (ssi_2_effectiveness = 'Yes')             as ssi2_effective,
           (ssi_3_effectiveness = 'Yes')             as ssi3_effective,

           -- Practice Indicators (PP & GP)
           (ssi_lit_1 = '1')                         as pp_lit_1,
           (ssi_lit_2 = '1')                         as pp_lit_2,
           (ssi_lit_3 = '1')                         as pp_lit_3,
           (ssi_lit_4 = '1')                         as pp_lit_4,
           (ssi_num_1 = '1')                         as pp_num_1,
           (ssi_num_2 = '1')                         as pp_num_2,
           (ssi_num_3 = '1')                         as pp_num_3,
           (ssi_num_4 = '1')                         as pp_num_4,
           q4_4,
           q4_8,
           q3_h_7,

           -- Teacher Guide
           (q3 = 'Yes')                              as tg_available,
           CASE
               WHEN q3 = 'Yes' THEN 'All Steps'
               WHEN q3 = 'No' THEN 'No Steps'
               ELSE 'Partial Steps'
               END                                   as tg_followed,

           -- Student Counts
           COALESCE(enrolled_students::int, 0)       as enrolled_students,
           COALESCE(present_students::int, 0)        as present_students,
           COALESCE(enrolled_students_boys::int, 0)  as enrolled_boys,
           COALESCE(enrolled_students_girls::int, 0) as enrolled_girls,
           COALESCE(present_students_boys::int, 0)   as present_boys,
           COALESCE(present_students_girls::int, 0)  as present_girls,

           -- Workbook & Tracker
           wb_avail,
           CONCAT_WS('', q3_h_9, q3_m_4)             as wb_usage,
           CONCAT_WS('', q3_h_10, q3_m_5)            as wb_checked,
           CONCAT_WS('', q3_h_11, q3_m_6)            as tracker_filled,

           -- Demo & Remedial
           q6_1                                      as demo_done,
           q6_1a                                     as demo_done_by,
           q4_5                                      as remedial_done,

           -- Class Situation
           CASE
               WHEN class_situation = 'sg' THEN 'Single Grade'
               WHEN class_situation = 'mg' THEN 'Multi Grade'
               ELSE 'Other'
               END                                   as class_situation,

           -- Personnel
           staff_name                                as arp_name,
           teacher_gender,
           school                                    as school_name

    FROM surveycto_gsheet_data.haryana_cro_tool_2025_26

    UNION ALL

    -- UP CRO Data (same structure)
    SELECT 'uttar pradesh'                           as state,
           LOWER(district)                           as district,
           LOWER(block)                              as block,
           ay,
           TRIM(TO_CHAR(visit_date, 'Month'))        as month,
           subject,
           ('Grade ' || class)                       as grade,
           COALESCE(visit_type, 'Individual')        as visit_type,

           (ssi_2_effectiveness = 'Yes')             as ssi2_effective,
           (ssi_3_effectiveness = 'Yes')             as ssi3_effective,

           (ssi_lit_1 = '1')                         as pp_lit_1,
           (ssi_lit_2 = '1')                         as pp_lit_2,
           (ssi_lit_3 = '1')                         as pp_lit_3,
           (ssi_lit_4 = '1')                         as pp_lit_4,
           (ssi_num_1 = '1')                         as pp_num_1,
           (ssi_num_2 = '1')                         as pp_num_2,
           (ssi_num_3 = '1')                         as pp_num_3,
           (ssi_num_4 = '1')                         as pp_num_4,
           q4_4,
           q4_8,
           q3_h_7,

           (q3 = 'Yes')                              as tg_available,
           CASE
               WHEN q3 = 'Yes' THEN 'All Steps'
               WHEN q3 = 'No' THEN 'No Steps'
               ELSE 'Partial Steps'
               END                                   as tg_followed,

           COALESCE(enrolled_students::int, 0)       as enrolled_students,
           COALESCE(present_students::int, 0)        as present_students,
           COALESCE(enrolled_students_boys::int, 0)  as enrolled_boys,
           COALESCE(enrolled_students_girls::int, 0) as enrolled_girls,
           COALESCE(present_students_boys::int, 0)   as present_boys,
           COALESCE(present_students_girls::int, 0)  as present_girls,

           wb_avail,
           CONCAT_WS('', q3_h_9, q3_m_4)             as wb_usage,
           CONCAT_WS('', q3_h_10, q3_m_5)            as wb_checked,
           CONCAT_WS('', q3_h_11, q3_m_6)            as tracker_filled,

           q6_1                                      as demo_done,
           q6_1a                                     as demo_done_by,
           q4_5                                      as remedial_done,

           CASE
               WHEN class_situation = 'sg' THEN 'Single Grade'
               WHEN class_situation = 'mg' THEN 'Multi Grade'
               ELSE 'Other'
               END                                   as class_situation,

           staff_name                                as arp_name,
           teacher_gender,
           school                                    as school_name

    FROM surveycto_gsheet_data.up_cro_tool_2025_2026)

SELECT
    -- Total Counts
    COUNT(*)                    as total_observations,
    COUNT(DISTINCT school_name) as unique_schools,
    COUNT(DISTINCT arp_name)    as unique_arps,

    -- Grade-wise Distribution
    json_build_object(
            'Grade 1', COUNT(*) FILTER (WHERE grade = 'Grade 1'),
            'Grade 2', COUNT(*) FILTER (WHERE grade = 'Grade 2'),
            'Grade 3', COUNT(*) FILTER (WHERE grade = 'Grade 3')
    )                           as grade_distribution,

    -- Subject-wise Distribution
    json_build_object(
            'Literacy', COUNT(*) FILTER (WHERE LOWER(subject) = 'Hindi'),
            'Numeracy', COUNT(*) FILTER (WHERE LOWER(subject) = 'Math')
    )                           as subject_distribution,

    -- Visit Type Distribution
    json_build_object(
            'Individual', COUNT(*) FILTER (WHERE visit_type = 'Individual'),
            'Joint', COUNT(*) FILTER (WHERE visit_type = 'Joint')
    )                           as visit_type_distribution,

    -- SSI Effectiveness
    json_build_object(
            'SSI-2 Effective', ROUND(COUNT(*) FILTER (WHERE ssi2_effective)::numeric / NULLIF(COUNT(*), 0) * 100, 1),
            'SSI-3 Effective', ROUND(COUNT(*) FILTER (WHERE ssi3_effective)::numeric / NULLIF(COUNT(*), 0) * 100, 1)
    )                           as ssi_effectiveness,

    json_build_object(

            'pp_lit_1',
            ROUND(
                    COUNT(*) FILTER (WHERE pp_lit_1)::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Hindi'), 0) * 100, 1
            ),
            'pp_lit_2',
            ROUND(
                    COUNT(*) FILTER (WHERE pp_lit_2)::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Hindi'), 0) * 100, 1
            ),
            'pp_lit_3',
            ROUND(
                    COUNT(*) FILTER (WHERE pp_lit_3)::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Hindi'), 0) * 100, 1
            ),
            'pp_lit_4',
            ROUND(
                    COUNT(*) FILTER (WHERE pp_lit_4)::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Hindi'), 0) * 100, 1
            ),
            'pp_num_1',
            ROUND(
                    COUNT(*) FILTER (WHERE pp_num_1)::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Math'), 0) * 100, 1
            ),
            'pp_num_2',
            ROUND(
                    COUNT(*) FILTER (WHERE pp_num_2)::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Math'), 0) * 100, 1
            ),
            'pp_num_3',
            ROUND(
                    COUNT(*) FILTER (WHERE pp_num_3)::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Math'), 0) * 100, 1
            ),
            'pp_num_4',
            ROUND(
                    COUNT(*) FILTER (WHERE pp_num_4)::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Math'), 0) * 100, 1
            ),

            'gp_lit_1',
            ROUND(
                    COUNT(*) FILTER (WHERE subject = 'Hindi' AND q4_4 = 'Yes')::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Hindi'), 0) * 100, 1
            ),
            'gp_lit_2',
            ROUND(
                    COUNT(*) FILTER (WHERE subject = 'Hindi' AND q4_8 = 'Yes')::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Hindi'), 0) * 100, 1
            ),
            'gp_lit_3',
            ROUND(
                    COUNT(*) FILTER (WHERE subject = 'Hindi' AND q3_h_7 = 'Yes')::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Hindi'), 0) * 100, 1
            ),
            'gp_num_1',
            ROUND(
                    COUNT(*) FILTER (WHERE subject = 'Math' AND q4_4 = 'Yes')::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Math'), 0) * 100, 1
            ),
            'gp_num_2',
            ROUND(
                    COUNT(*) FILTER (WHERE subject = 'Math' AND q4_8 = 'Yes')::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Math'), 0) * 100, 1
            ),
            'gp_num_3',
            ROUND(
                    COUNT(*) FILTER (WHERE subject = 'Math' AND q3_h_7 = 'Yes')::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE subject = 'Math'), 0) * 100, 1
            )
    )                           AS practice_indicators,

    -- Teacher Guide Stats
    json_build_object(
            'Available', COUNT(*) FILTER (WHERE tg_available),
            'All Steps', COUNT(*) FILTER (WHERE tg_followed = 'All Steps'),
            'Partial Steps', COUNT(*) FILTER (WHERE tg_followed = 'Partial Steps'),
            'No Steps', COUNT(*) FILTER (WHERE tg_followed = 'No Steps')
    )                           as teacher_guide_stats,

    -- Student Enrollment & Attendance
    json_build_object(
            'total_enrolled', SUM(enrolled_students),
            'total_present', SUM(present_students),
            'boys_enrolled', SUM(enrolled_boys),
            'girls_enrolled', SUM(enrolled_girls),
            'boys_present', SUM(present_boys),
            'girls_present', SUM(present_girls),
            'attendance_rate', ROUND(SUM(present_students)::numeric / NULLIF(SUM(enrolled_students), 0) * 100, 1)
    )                           as student_stats,

    -- Workbook & Tracker
    json_build_object(
            'wb_available', COUNT(*) FILTER (WHERE wb_avail = 'Yes'),
            'wb_used', COUNT(*) FILTER (WHERE wb_usage = 'Yes'),
            'wb_checked', COUNT(*) FILTER (WHERE wb_checked = 'Yes'),
            'tracker_filled', COUNT(*) FILTER (WHERE tracker_filled = 'Yes')
    )                           as workbook_tracker_stats,

    -- Demo & Remedial
    json_build_object(
            'demo_conducted', COUNT(*) FILTER (WHERE demo_done = 'Yes'),
            'remedial_done', COUNT(*) FILTER (WHERE remedial_done = 'Yes')
    )                           as intervention_stats,

    -- Class Situation
    json_build_object(
            'Single Grade', COUNT(*) FILTER (WHERE class_situation = 'Single Grade'),
            'Multi Grade', COUNT(*) FILTER (WHERE class_situation = 'Multi Grade'),
            'Other', COUNT(*) FILTER (WHERE class_situation = 'Other')
    )                           as class_situation_distribution,

    -- Teacher Gender Distribution
    json_build_object(
            'Male', COUNT(*) FILTER (WHERE teacher_gender = 'Male'),
            'Female', COUNT(*) FILTER (WHERE teacher_gender = 'Female')
    )                           as teacher_gender_distribution

FROM unified_cro
    ${whereClause}
  `;

  try {
    const result = await db.query(query, params);
    return result.rows[0];
  } catch (error) {
    console.error("CRO Metrics error:", error);
    throw error;
  }
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
  const { state, district, block, year, subject, grade, visit_type, month } =
    filters;

  const buildWhereClause = (params, alias = "") => {
    const conditions = [];
    let paramIndex = params.length + 1;
    const prefix = alias ? `${alias}.` : "";

    if (state && state !== "All") {
      conditions.push(`${prefix}state = $${paramIndex++}`);
      params.push(state.toLowerCase());
    }
    if (district && district !== "All") {
      conditions.push(`${prefix}district = $${paramIndex++}`);
      params.push(district.toLowerCase());
    }
    if (block && block !== "All") {
      conditions.push(`${prefix}block = $${paramIndex++}`);
      params.push(block.toLowerCase());
    }
    if (subject && subject !== "All") {
      conditions.push(`LOWER(${prefix}subject) = $${paramIndex++}`);
      params.push(subject.toLowerCase());
    }
    if (grade && grade !== "All") {
      conditions.push(`${prefix}grade = $${paramIndex++}`);
      params.push(grade);
    }
    if (visit_type && visit_type !== "All") {
      conditions.push(`${prefix}visit_type = $${paramIndex++}`);
      params.push(visit_type);
    }
    if (year && year !== "All") {
      conditions.push(`${prefix}ay = $${paramIndex++}`);
      params.push(year);
    }
    if (month && month !== "All") {
      conditions.push(`TRIM(${prefix}month) = $${paramIndex++}`);
      params.push(month.trim());
    }

    return conditions.length > 0 ? "WHERE " + conditions.join(" AND ") : "";
  };

  const params = [];

  // ✅ OPTIMIZATION 1: Pre-join targets ONCE instead of per-row correlated subquery
  const summaryQuery = `
    WITH 
    -- Pre-aggregate targets by BAC-Month-State (eliminates correlated subquery)
    target_lookup AS (
      SELECT 
        LOWER(staff_name) as bac_name,
        LOWER(state) as state,
        DATE_TRUNC('month', visit_month_year) as month_date,
        MAX(total_visit_days) as target_visits
      FROM master_targets.staff_monthly_work_plan
      GROUP BY LOWER(staff_name), LOWER(state), DATE_TRUNC('month', visit_month_year)
    ),
    
    -- ✅ OPTIMIZATION 2: Single unified query with LEFT JOIN (no correlated subquery)
    unified_data AS (
      -- Haryana Data
      SELECT
        h.staff_name as bac_name,
        'haryana' as state,
        LOWER(h.district) as district,
        LOWER(h.block) as block,
        h.visit_date,
        h.ay,
        TRIM(TO_CHAR(h.visit_date, 'Month')) as month,
        EXTRACT(MONTH FROM h.visit_date)::int as month_num,
        h.subject,
        ('Grade ' || h.class) as grade,
        COALESCE(h.visit_type, 'Individual') as visit_type,
        COALESCE(t.target_visits, 15) as target_visits,
        15 as recommended_visits
      FROM surveycto_gsheet_data.haryana_cro_tool_2025_26 h
      LEFT JOIN target_lookup t 
        ON LOWER(h.staff_name) = t.bac_name
        AND t.state = 'haryana'
        AND DATE_TRUNC('month', h.visit_date) = t.month_date

      UNION ALL

      -- Uttar Pradesh Data
      SELECT
        u.staff_name as bac_name,
        'uttar pradesh' as state,
        LOWER(u.district) as district,
        LOWER(u.block) as block,
        u.visit_date,
        u.ay,
        TRIM(TO_CHAR(u.visit_date, 'Month')) as month,
        EXTRACT(MONTH FROM u.visit_date)::int as month_num,
        u.subject,
        ('Grade ' || u.class) as grade,
        COALESCE(u.visit_type, 'Individual') as visit_type,
        COALESCE(t.target_visits, 15) as target_visits,
        15 as recommended_visits
      FROM surveycto_gsheet_data.up_cro_tool_2025_2026 u
      LEFT JOIN target_lookup t 
        ON LOWER(u.staff_name) = t.bac_name
        AND t.state = 'uttar pradesh'
        AND DATE_TRUNC('month', u.visit_date) = t.month_date
    ),
    
    -- ✅ OPTIMIZATION 3: Calculate BAC-month metrics (count unique visit days per BAC-month)
    bac_monthly AS (
      SELECT 
        bac_name,
        month,
        month_num,
        COUNT(DISTINCT visit_date) as month_actual_visits,
        MAX(target_visits) as month_target_visits,
        MAX(recommended_visits) as month_recommended_visits
      FROM unified_data
      ${buildWhereClause(params, "unified_data")}
      GROUP BY bac_name, month, month_num
    ),
    
    -- ✅ OPTIMIZATION 4: Single aggregation for all BAC totals
    bac_totals AS (
      SELECT
        bac_name,
        SUM(month_actual_visits) as total_actual,
        SUM(month_target_visits) as total_target,
        SUM(month_recommended_visits) as total_recommended,
        SUM(CASE WHEN month_actual_visits < month_target_visits THEN 1 ELSE 0 END) as months_missed,
        SUM(CASE WHEN month_target_visits < month_recommended_visits THEN 1 ELSE 0 END) as months_underplanned,
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
    
    -- ✅ OPTIMIZATION 5: Monthly aggregates (already filtered data, no re-filtering)
    month_level AS (
      SELECT 
        month,
        month_num,
        SUM(month_actual_visits) as month_actual,
        SUM(month_target_visits) as month_target,
        SUM(month_recommended_visits) as month_recommended
      FROM bac_monthly
      GROUP BY month, month_num
      ORDER BY month_num
    ),
    
    -- Yearly aggregates (from filtered unified_data)
    year_level AS (
      SELECT 
        ay as year,
        COUNT(DISTINCT visit_date) as year_actual,
        SUM(target_visits) as year_target,
        SUM(recommended_visits) as year_recommended
      FROM unified_data
      ${buildWhereClause(params, "unified_data")}
      GROUP BY ay
      ORDER BY ay
    ),
    
    -- ✅ OPTIMIZATION 6: Calculate ALL metrics in ONE pass using aggregates
    final_metrics AS (
      SELECT
        -- BAC-level metrics (pre-calculated, just aggregate)
        COUNT(*) FILTER (WHERE total_actual < total_target) as missed_target_bacs,
        COUNT(*) FILTER (WHERE total_target != total_recommended) as fluctuated_target_bacs,
        SUM(total_actual) as total_actual,
        SUM(total_target) as total_target,
        SUM(total_recommended) as total_recommended,
        COUNT(*) FILTER (WHERE months_missed >= 3) as chronic_underperformers,
        COUNT(*) FILTER (WHERE months_underplanned >= 3) as chronic_underplanners,
        COUNT(*) as total_bacs,
        
        -- Performance distribution
        COUNT(*) FILTER (WHERE achievement_pct >= 100) as high_performers,
        COUNT(*) FILTER (WHERE achievement_pct >= 80 AND achievement_pct < 100) as medium_performers,
        COUNT(*) FILTER (WHERE achievement_pct < 80) as low_performers,
        
        -- Planning distribution
        COUNT(*) FILTER (WHERE planning_pct >= 100) as full_planners,
        COUNT(*) FILTER (WHERE planning_pct >= 80 AND planning_pct < 100) as partial_planners,
        COUNT(*) FILTER (WHERE planning_pct < 80) as under_planners
      FROM bac_totals
    )
    
    -- ✅ OPTIMIZATION 7: Single final SELECT (no subqueries)
    SELECT 
      fm.*,
      (SELECT json_agg(json_build_object(
        'month', month,
        'actual', month_actual,
        'target', month_target,
        'recommended', month_recommended
      ) ORDER BY month_num) FROM month_level) as monthly_data,
      (SELECT json_agg(json_build_object(
        'year', year,
        'actual', year_actual,
        'target', year_target,
        'recommended', year_recommended
      ) ORDER BY year) FROM year_level) as yearly_data
    FROM final_metrics fm
  `;

  try {
    const result = await db.query(summaryQuery, params);
    const row = result.rows[0];

    const actualAchievement =
      row.total_target > 0 ? (row.total_actual / row.total_target) * 100 : 0;

    const targetVsPolicy =
      row.total_recommended > 0
        ? (row.total_target / row.total_recommended) * 100
        : 0;

    const avgAchievement =
      row.total_target > 0 ? (row.total_actual / row.total_target) * 100 : 0;

    const avgPlanning =
      row.total_recommended > 0
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
        totalGap: parseInt(totalGap),
      },
      charts: {
        monthly: row.monthly_data || [],
        yearly: row.yearly_data || [],
        performanceDistribution: [
          { name: "High (≥100%)", value: parseInt(row.high_performers) || 0 },
          {
            name: "Medium (80-99%)",
            value: parseInt(row.medium_performers) || 0,
          },
          { name: "Low (<80%)", value: parseInt(row.low_performers) || 0 },
        ],
        planningDistribution: [
          { name: "Full (≥100%)", value: parseInt(row.full_planners) || 0 },
          {
            name: "Partial (80-99%)",
            value: parseInt(row.partial_planners) || 0,
          },
          { name: "Under (<80%)", value: parseInt(row.under_planners) || 0 },
        ],
      },
      meta: {
        totalActual: parseInt(row.total_actual) || 0,
        totalTarget: parseInt(row.total_target) || 0,
        totalRecommended: parseInt(row.total_recommended) || 0,
      },
    };
  } catch (error) {
    console.error("Summary metrics error:", error);
    throw error;
  }
};

/**
 * OPTIMIZED: Get chronic performers list (only when needed)
 */
const getChronicPerformers = async (filters, threshold = 3) => {
  const { state, district, block, year, month } = filters;

  const params = [threshold];
  let paramIndex = 2;
  const conditions = [];

  if (state && state !== "All") {
    conditions.push(`u.state = $${paramIndex++}`);
    params.push(state.toLowerCase());
  }
  if (district && district !== "All") {
    conditions.push(`u.district = $${paramIndex++}`);
    params.push(district.toLowerCase());
  }
  if (block && block !== "All") {
    conditions.push(`u.block = $${paramIndex++}`);
    params.push(block.toLowerCase());
  }
  if (year && year !== "All") {
    conditions.push(`u.ay = $${paramIndex++}`);
    params.push(year);
  }
  if (month && month !== "All") {
    conditions.push(`TRIM(u.month) = $${paramIndex++}`);
    params.push(month.trim());
  }

  const whereClause =
    conditions.length > 0 ? "AND " + conditions.join(" AND ") : "";

  // ✅ OPTIMIZED: Pre-join targets, calculate per BAC-month, then aggregate
  const query = `
    WITH 
    -- Pre-aggregate targets (eliminates correlated subquery)
    target_lookup AS (
      SELECT 
        LOWER(staff_name) as bac_name,
        LOWER(state) as state,
        DATE_TRUNC('month', visit_month_year) as month_date,
        MAX(total_visit_days) as target_visits
      FROM master_targets.staff_monthly_work_plan
      GROUP BY LOWER(staff_name), LOWER(state), DATE_TRUNC('month', visit_month_year)
    ),
    
    unified_data AS (
      -- Haryana
      SELECT
        h.staff_name as bac_name,
        'haryana' as state,
        LOWER(h.district) as district,
        LOWER(h.block) as block,
        h.visit_date,
        h.ay,
        TRIM(TO_CHAR(h.visit_date, 'Month')) as month,
        COALESCE(t.target_visits, 15) as target_visits
      FROM surveycto_gsheet_data.haryana_cro_tool_2025_26 h
      LEFT JOIN target_lookup t 
        ON LOWER(h.staff_name) = t.bac_name
        AND t.state = 'haryana'
        AND DATE_TRUNC('month', h.visit_date) = t.month_date
      
      UNION ALL
      
      -- UP
      SELECT
        u.staff_name as bac_name,
        'uttar pradesh' as state,
        LOWER(u.district) as district,
        LOWER(u.block) as block,
        u.visit_date,
        u.ay,
        TRIM(TO_CHAR(u.visit_date, 'Month')) as month,
        COALESCE(t.target_visits, 15) as target_visits
      FROM surveycto_gsheet_data.up_cro_tool_2025_2026 u
      LEFT JOIN target_lookup t 
        ON LOWER(u.staff_name) = t.bac_name
        AND t.state = 'uttar pradesh'
        AND DATE_TRUNC('month', u.visit_date) = t.month_date
    ),
    
    -- Calculate per BAC-month
    bac_monthly AS (
      SELECT 
        bac_name,
        state,
        district,
        block,
        month,
        COUNT(DISTINCT visit_date) as month_actual,
        MAX(target_visits) as month_target
      FROM unified_data u
      WHERE 1=1 ${whereClause}
      GROUP BY bac_name, state, district, block, month
    ),
    
    -- Aggregate to BAC level
    bac_totals AS (
      SELECT 
        bac_name,
        state,
        district,
        block,
        SUM(month_actual) as total_actual,
        SUM(month_target) as total_target,
        SUM(CASE WHEN month_actual < month_target THEN 1 ELSE 0 END) as months_missed,
        CASE 
          WHEN SUM(month_target) > 0 
          THEN ROUND((SUM(month_actual)::float / SUM(month_target) * 100)::numeric, 1)
          ELSE 0 
        END as avg_achievement
      FROM bac_monthly
      GROUP BY bac_name, state, district, block
      HAVING SUM(CASE WHEN month_actual < month_target THEN 1 ELSE 0 END) >= $1
    )
    
    SELECT 
      bac_name,
      state,
      district,
      block,
      months_missed,
      total_actual,
      total_target,
      avg_achievement,
      CASE 
        WHEN avg_achievement < 70 THEN 'critical'
        ELSE 'warning'
      END as status
    FROM bac_totals
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
  const { state, district, block, year } = filters;

  const params = [threshold];
  let paramIndex = 2;
  const conditions = [];

  if (state && state !== "All") {
    conditions.push(`u.state = $${paramIndex++}`);
    params.push(state.toLowerCase());
  }
  if (district && district !== "All") {
    conditions.push(`u.district = $${paramIndex++}`);
    params.push(district.toLowerCase());
  }
  if (block && block !== "All") {
    conditions.push(`u.block = $${paramIndex++}`);
    params.push(block.toLowerCase());
  }
  if (year && year !== "All") {
    conditions.push(`u.ay = $${paramIndex++}`);
    params.push(year);
  }

  const whereClause =
    conditions.length > 0 ? "AND " + conditions.join(" AND ") : "";

  // ✅ OPTIMIZED: Pre-join targets, calculate planning per BAC-month
  const query = `
    WITH 
    -- Pre-aggregate targets
    target_lookup AS (
      SELECT 
        LOWER(staff_name) as bac_name,
        LOWER(state) as state,
        DATE_TRUNC('month', visit_month_year) as month_date,
        MAX(total_visit_days) as target_visits
      FROM master_targets.staff_monthly_work_plan
      GROUP BY LOWER(staff_name), LOWER(state), DATE_TRUNC('month', visit_month_year)
    ),
    
    unified_data AS (
      -- Haryana
      SELECT
        h.staff_name as bac_name,
        'haryana' as state,
        LOWER(h.district) as district,
        LOWER(h.block) as block,
        h.ay,
        TRIM(TO_CHAR(h.visit_date, 'Month')) as month,
        COALESCE(t.target_visits, 15) as target_visits,
        15 as recommended_visits
      FROM surveycto_gsheet_data.haryana_cro_tool_2025_26 h
      LEFT JOIN target_lookup t 
        ON LOWER(h.staff_name) = t.bac_name
        AND t.state = 'haryana'
        AND DATE_TRUNC('month', h.visit_date) = t.month_date
      
      UNION ALL
      
      -- UP
      SELECT
        u.staff_name as bac_name,
        'uttar pradesh' as state,
        LOWER(u.district) as district,
        LOWER(u.block) as block,
        u.ay,
        TRIM(TO_CHAR(u.visit_date, 'Month')) as month,
        COALESCE(t.target_visits, 15) as target_visits,
        15 as recommended_visits
      FROM surveycto_gsheet_data.up_cro_tool_2025_2026 u
      LEFT JOIN target_lookup t 
        ON LOWER(u.staff_name) = t.bac_name
        AND t.state = 'uttar pradesh'
        AND DATE_TRUNC('month', u.visit_date) = t.month_date
    ),
    
    -- Calculate per BAC-month
    bac_monthly AS (
      SELECT 
        bac_name,
        state,
        district,
        block,
        month,
        MAX(target_visits) as month_target,
        MAX(recommended_visits) as month_recommended
      FROM unified_data u
      WHERE 1=1 ${whereClause}
      GROUP BY bac_name, state, district, block, month
    ),
    
    -- Aggregate to BAC level
    bac_totals AS (
      SELECT 
        bac_name,
        state,
        district,
        block,
        SUM(month_target) as total_target,
        SUM(month_recommended) as total_recommended,
        SUM(CASE WHEN month_target < month_recommended THEN 1 ELSE 0 END) as months_underplanned,
        CASE 
          WHEN SUM(month_recommended) > 0 
          THEN ROUND((SUM(month_target)::float / SUM(month_recommended) * 100)::numeric, 1)
          ELSE 0 
        END as avg_planning
      FROM bac_monthly
      GROUP BY bac_name, state, district, block
      HAVING SUM(CASE WHEN month_target < month_recommended THEN 1 ELSE 0 END) >= $1
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
      avg_planning,
      CASE 
        WHEN avg_planning < 70 THEN 'critical'
        ELSE 'warning'
      END as status
    FROM bac_totals
    ORDER BY months_underplanned DESC, avg_planning ASC
    LIMIT 100;
  `;

  const result = await db.query(query, params);
  return result.rows;
};

module.exports = {
  getFilteredVisits,
  getSchoolsByBlock,
  getCROMetrics,
  getHierarchyMetrics,
  getNationalBoundaries,
  getStateBoundaries,
  getDistrictBoundaries,
  getBlockBoundaries,
  getSummaryMetrics,
  getChronicPerformers,
  getChronicPlanners,
};
