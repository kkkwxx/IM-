-- M2.1 消息存储：读扩散时间线模型，每会话一条时间线，消息只存一份。

CREATE TABLE IF NOT EXISTS messages (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,  -- 物理主键，仅内部使用
  conv_id    VARCHAR(64)   NOT NULL,                      -- 会话ID：单聊 "p:alice:bob"（uid排序拼接）
  seq        BIGINT        NOT NULL,                      -- 会话内逻辑序号，客户端同步的进度刻度
  msg_id     CHAR(16)      NOT NULL,                      -- 客户端生成的随机ID，幂等键
  sender     VARCHAR(32)   NOT NULL,
  content    VARCHAR(4096) NOT NULL,
  created_at DATETIME(3)   NOT NULL,                      -- 仅展示用，不参与排序
  UNIQUE KEY uk_conv_seq (conv_id, seq),                  -- 保序：同会话不允许重号
  UNIQUE KEY uk_conv_msg (conv_id, msg_id)                -- 幂等：同 msg_id 重试直接冲突
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- M2.4 鉴权
CREATE TABLE IF NOT EXISTS users (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  username      VARCHAR(32)  NOT NULL,
  password_hash CHAR(60)     NOT NULL,                     -- bcrypt，自带盐
  created_at    DATETIME(3)  NOT NULL,
  UNIQUE KEY uk_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- refresh token 只存 SHA-256 哈希：库泄露也拿不到可用 token；可吊销是它相对 JWT 的核心价值
CREATE TABLE IF NOT EXISTS refresh_tokens (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  uid        VARCHAR(32)  NOT NULL,
  token_hash CHAR(64)     NOT NULL,
  expires_at DATETIME(3)  NOT NULL,
  created_at DATETIME(3)  NOT NULL,
  UNIQUE KEY uk_token_hash (token_hash),
  KEY idx_uid (uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;