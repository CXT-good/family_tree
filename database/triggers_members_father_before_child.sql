-- =============================================================================
-- 成员表高级约束：父亲出生日必须早于子女出生日（子女生日 <= 父亲生日则拒绝）
-- MySQL 8+，库名 FamilyTreeDB，表名 members（列 father_member_id / birth_date）
-- 用法：在 Navicat 或 mysql 客户端整段执行（含 DELIMITER）
-- =============================================================================

USE FamilyTreeDB;

DROP TRIGGER IF EXISTS members_bi_father_before_child;
DROP TRIGGER IF EXISTS members_bu_father_before_child;

DELIMITER $$

-- INSERT：插入前校验
CREATE TRIGGER members_bi_father_before_child
BEFORE INSERT ON members
FOR EACH ROW
BEGIN
  DECLARE father_bd DATE;

  IF NEW.father_member_id IS NOT NULL AND NEW.birth_date IS NOT NULL THEN
    SELECT birth_date INTO father_bd
    FROM members
    WHERE member_id = NEW.father_member_id
    LIMIT 1;

    IF father_bd IS NOT NULL THEN
      IF NEW.birth_date <= father_bd THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = '违反血缘约束：子女出生日必须晚于父亲出生日（父亲 member_id 指向的生辰不得晚于子女）';
      END IF;
    END IF;
  END IF;
END$$

-- UPDATE：修改生日或父亲时重新校验
CREATE TRIGGER members_bu_father_before_child
BEFORE UPDATE ON members
FOR EACH ROW
BEGIN
  DECLARE father_bd DATE;

  IF NEW.father_member_id IS NOT NULL AND NEW.birth_date IS NOT NULL THEN
    SELECT birth_date INTO father_bd
    FROM members
    WHERE member_id = NEW.father_member_id
    LIMIT 1;

    IF father_bd IS NOT NULL THEN
      IF NEW.birth_date <= father_bd THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = '违反血缘约束：子女出生日必须晚于父亲出生日（父亲 member_id 指向的生辰不得晚于子女）';
      END IF;
    END IF;
  END IF;
END$$

DELIMITER ;

-- 验证示例（应失败）：
-- INSERT INTO members (member_id, tree_id, full_name, gender, birth_date, father_member_id, generation)
-- VALUES (9999999, 1, '测试', 'M', '2000-01-01', <某父亲id>, 9);
-- 若父亲 birth_date >= 2000-01-01 则会报错。
