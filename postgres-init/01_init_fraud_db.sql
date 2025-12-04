-- =============================================================================
-- FinTech Fraud Detection Pipeline - PostgreSQL Initialization
-- =============================================================================
-- This script creates the necessary tables for the fraud detection system.
-- It runs automatically when the PostgreSQL container starts.
-- =============================================================================

-- Create fraud_alerts table to store detected fraudulent transactions
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100),
    user_id VARCHAR(50) NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    merchant_category VARCHAR(100),
    amount DECIMAL(15, 2),
    location TEXT,
    country TEXT,
    fraud_type VARCHAR(50) NOT NULL,
    fraud_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for efficient querying
    CONSTRAINT fraud_alerts_fraud_type_check 
        CHECK (fraud_type IN ('HIGH_VALUE', 'IMPOSSIBLE_TRAVEL', 'OTHER'))
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_user_id ON fraud_alerts(user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_detected_at ON fraud_alerts(detected_at);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_fraud_type ON fraud_alerts(fraud_type);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_transaction_id ON fraud_alerts(transaction_id);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_merchant ON fraud_alerts(merchant_category);

-- Create a view for fraud statistics
CREATE OR REPLACE VIEW fraud_statistics AS
SELECT 
    fraud_type,
    COUNT(*) as total_alerts,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(detected_at) as first_detected,
    MAX(detected_at) as last_detected
FROM fraud_alerts
GROUP BY fraud_type;

-- Create a view for daily fraud summary
CREATE OR REPLACE VIEW daily_fraud_summary AS
SELECT 
    DATE(detected_at) as fraud_date,
    fraud_type,
    COUNT(*) as alert_count,
    SUM(amount) as total_amount,
    COUNT(DISTINCT user_id) as unique_users
FROM fraud_alerts
GROUP BY DATE(detected_at), fraud_type
ORDER BY fraud_date DESC, fraud_type;

-- Create a view for merchant category analysis
CREATE OR REPLACE VIEW merchant_fraud_analysis AS
SELECT 
    merchant_category,
    COUNT(*) as fraud_count,
    SUM(amount) as fraud_amount,
    AVG(amount) as avg_fraud_amount,
    COUNT(DISTINCT user_id) as affected_users
FROM fraud_alerts
WHERE merchant_category IS NOT NULL AND merchant_category != 'unknown'
GROUP BY merchant_category
ORDER BY fraud_count DESC;

-- Grant permissions (if needed for different users)
-- GRANT SELECT ON fraud_alerts TO readonly_user;
-- GRANT ALL ON fraud_alerts TO fintech;

-- Insert a test record to verify table creation (optional - can be removed)
-- INSERT INTO fraud_alerts (transaction_id, user_id, detected_at, merchant_category, amount, location, country, fraud_type, fraud_reason)
-- VALUES ('TEST-001', 'USER_TEST', NOW(), 'test_category', 9999.99, 'Test City, Test Country', 'Test Country', 'HIGH_VALUE', 'Test record - can be deleted');

COMMENT ON TABLE fraud_alerts IS 'Stores all detected fraudulent transactions from the real-time fraud detection system';
COMMENT ON COLUMN fraud_alerts.fraud_type IS 'Type of fraud: HIGH_VALUE (>$5000) or IMPOSSIBLE_TRAVEL (same user in different countries within 10 min)';
COMMENT ON COLUMN fraud_alerts.detected_at IS 'Timestamp when the fraud was detected by the streaming system';

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE 'FinTech Fraud Detection database initialized successfully!';
    RAISE NOTICE 'Tables created: fraud_alerts';
    RAISE NOTICE 'Views created: fraud_statistics, daily_fraud_summary, merchant_fraud_analysis';
END $$;
