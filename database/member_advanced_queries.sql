-- 族谱成员高级查询（每条需求一条 SQL，与 MemberQueriesController 一致）
-- 使用前：SET @treeId = 1; SET @memberId = 100;

USE FamilyTreeDB;

-- 1. 基本查询：配偶 + 全部子女
SELECT
  '配偶' AS RelationKind,
  m.member_id, m.full_name, m.gender, m.generation, m.birth_date, m.death_date
FROM marriages mar
INNER JOIN members m
  ON m.tree_id = mar.tree_id
 AND m.member_id = (CASE WHEN mar.husband_id = @memberId THEN mar.wife_id ELSE mar.husband_id END)
WHERE mar.tree_id = @treeId
  AND (mar.husband_id = @memberId OR mar.wife_id = @memberId)
UNION ALL
SELECT
  '子女', c.member_id, c.full_name, c.gender, c.generation, c.birth_date, c.death_date
FROM members c
WHERE c.tree_id = @treeId
  AND (c.father_member_id = @memberId OR c.mother_member_id = @memberId)
ORDER BY RelationKind, member_id;

-- 2. 平均寿命最长的一代人
SELECT generation,
       AVG(DATEDIFF(death_date, birth_date) / 365.25) AS avg_lifespan_years,
       COUNT(*) AS member_count
FROM members
WHERE tree_id = @treeId
  AND birth_date IS NOT NULL AND death_date IS NOT NULL AND generation IS NOT NULL
GROUP BY generation
ORDER BY avg_lifespan_years DESC
LIMIT 1;

-- 3. 年龄>50 且无配偶的男性（索引友好写法）
SELECT m.member_id, m.full_name, TIMESTAMPDIFF(YEAR, m.birth_date, CURDATE()) AS age_years
FROM members m
WHERE m.tree_id = @treeId
  AND m.gender = 'M'
  AND m.birth_date IS NOT NULL
  AND m.birth_date <= DATE_SUB(CURDATE(), INTERVAL 50 YEAR)
  AND NOT EXISTS (
    SELECT 1 FROM marriages mar
    WHERE mar.tree_id = @treeId AND mar.husband_id = m.member_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM marriages mar
    WHERE mar.tree_id = @treeId AND mar.wife_id = m.member_id
  )
ORDER BY m.birth_date ASC, m.member_id
LIMIT 40 OFFSET 0;

-- 4. 出生年份早于同辈平均出生年份
SELECT m.member_id, m.full_name, m.generation,
       YEAR(m.birth_date) AS birth_year, g.avg_birth_year
FROM members m
INNER JOIN (
  SELECT generation, AVG(YEAR(birth_date)) AS avg_birth_year
  FROM members
  WHERE tree_id = @treeId AND generation IS NOT NULL AND birth_date IS NOT NULL
  GROUP BY generation
) g ON g.generation = m.generation
WHERE m.tree_id = @treeId
  AND m.birth_date IS NOT NULL AND m.generation IS NOT NULL
  AND YEAR(m.birth_date) < g.avg_birth_year;
