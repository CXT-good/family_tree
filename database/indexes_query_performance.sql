-- 加速「50岁+无配偶男」等高级查询（在 FamilyTreeDB 中执行一次即可）
USE FamilyTreeDB;

-- 按族谱 + 性别 + 出生日期筛选男性
CREATE INDEX ix_members_tree_gender_birth
  ON members (tree_id, gender, birth_date);

-- 按族谱 + 丈夫/妻子查婚姻（配合 NOT EXISTS）
CREATE INDEX ix_marriages_tree_husband
  ON marriages (tree_id, husband_id);

CREATE INDEX ix_marriages_tree_wife
  ON marriages (tree_id, wife_id);
