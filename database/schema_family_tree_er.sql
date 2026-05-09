-- 与 ER 图一致的逻辑模型（MySQL 8+）
-- 命名：英文蛇形 + 注释对应中文业务名
-- 使用 IF NOT EXISTS：重复执行不会因「表已存在」报错；若需改表结构请另行 ALTER 或删库重建。

SET NAMES utf8mb4;

CREATE DATABASE IF NOT EXISTS FamilyTreeDB
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE FamilyTreeDB;

-- ========== 用户（多用户） ==========
CREATE TABLE IF NOT EXISTS users (
  user_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '用户ID',
  username      VARCHAR(64) NOT NULL COMMENT '用户名',
  password_hash VARCHAR(255) NOT NULL COMMENT '密码（存储哈希）',
  registered_at DATETIME NOT NULL COMMENT '注册时间',
  PRIMARY KEY (user_id),
  UNIQUE KEY uk_users_username (username)
) ENGINE=InnoDB COMMENT='用户';

-- ========== 族谱（一个家族一本谱） ==========
CREATE TABLE IF NOT EXISTS family_trees (
  tree_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '族谱ID',
  tree_name         VARCHAR(128) NOT NULL COMMENT '谱名',
  surname           VARCHAR(32) NOT NULL COMMENT '姓氏',
  created_by_user_id BIGINT UNSIGNED NOT NULL COMMENT '创建用户',
  revision_at       DATETIME NOT NULL COMMENT '修谱时间',
  PRIMARY KEY (tree_id),
  KEY ix_trees_creator (created_by_user_id),
  CONSTRAINT fk_trees_creator
    FOREIGN KEY (created_by_user_id) REFERENCES users (user_id)
    ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB COMMENT='族谱';

-- ========== 用户—族谱：协作/邀请（多对多「管理」） ==========
-- 创建者通常可在应用层同时写入一条 owner；被邀请用户写入 editor 等
CREATE TABLE IF NOT EXISTS tree_managers (
  tree_id    BIGINT UNSIGNED NOT NULL COMMENT '族谱ID',
  user_id    BIGINT UNSIGNED NOT NULL COMMENT '用户ID',
  role       ENUM('owner','editor','viewer') NOT NULL DEFAULT 'editor' COMMENT '角色',
  invited_at DATETIME NULL COMMENT '加入/邀请时间',
  PRIMARY KEY (tree_id, user_id),
  KEY ix_mgr_user (user_id),
  CONSTRAINT fk_mgr_tree FOREIGN KEY (tree_id) REFERENCES family_trees (tree_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_mgr_user FOREIGN KEY (user_id) REFERENCES users (user_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB COMMENT='族谱协作者';

-- ========== 成员（隶属于某一族谱） ==========
-- 血缘：通过 father_member_id / mother_member_id 自引用（父子/母女等）
CREATE TABLE IF NOT EXISTS members (
  member_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '成员ID',
  tree_id            BIGINT UNSIGNED NOT NULL COMMENT '所属族谱ID',
  full_name          VARCHAR(64) NOT NULL COMMENT '姓名',
  gender             CHAR(1) NOT NULL COMMENT '性别 M/F',
  birth_date         DATE NULL COMMENT '出生日期',
  death_date         DATE NULL COMMENT '死亡日期',
  biography          VARCHAR(512) NULL COMMENT '生平简介',
  father_member_id   BIGINT UNSIGNED NULL COMMENT '父亲成员ID',
  mother_member_id   BIGINT UNSIGNED NULL COMMENT '母亲成员ID',
  generation         INT UNSIGNED NULL COMMENT '辈分/代数（可选，便于展示）',
  PRIMARY KEY (member_id),
  KEY ix_members_tree (tree_id),
  KEY ix_members_father (father_member_id),
  KEY ix_members_mother (mother_member_id),
  CONSTRAINT fk_members_tree FOREIGN KEY (tree_id) REFERENCES family_trees (tree_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_members_father FOREIGN KEY (father_member_id) REFERENCES members (member_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_members_mother FOREIGN KEY (mother_member_id) REFERENCES members (member_id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB COMMENT='家族成员';

-- ========== 婚姻（成员之间多对多，落表实现） ==========
CREATE TABLE IF NOT EXISTS marriages (
  marriage_id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tree_id       BIGINT UNSIGNED NOT NULL COMMENT '族谱ID',
  husband_id    BIGINT UNSIGNED NOT NULL COMMENT '丈夫成员ID',
  wife_id       BIGINT UNSIGNED NOT NULL COMMENT '妻子成员ID',
  start_date    DATE NOT NULL COMMENT '结婚/关系开始时间',
  PRIMARY KEY (marriage_id),
  UNIQUE KEY uk_marriage_pair_tree (tree_id, husband_id, wife_id),
  KEY ix_marriage_husband (husband_id),
  KEY ix_marriage_wife (wife_id),
  CONSTRAINT fk_mar_tree FOREIGN KEY (tree_id) REFERENCES family_trees (tree_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_mar_husband FOREIGN KEY (husband_id) REFERENCES members (member_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_mar_wife FOREIGN KEY (wife_id) REFERENCES members (member_id)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB COMMENT='婚姻关系';
