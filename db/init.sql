CREATE TABLE IF NOT EXISTS user_logs_n1 (
    id BIGINT PRIMARY KEY,
    user_id INT NOT NULL,
    action VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_logs_n1_user_id
ON user_logs_n1(user_id);

CREATE TABLE IF NOT EXISTS user_logs_n2 (
    id BIGINT PRIMARY KEY,
    user_id INT NOT NULL,
    action VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_logs_n2_user_id
ON user_logs_n2(user_id);

CREATE TABLE IF NOT EXISTS user_logs_n4 (
    id BIGINT PRIMARY KEY,
    user_id INT NOT NULL,
    action VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_logs_n4_user_id
ON user_logs_n4(user_id);
