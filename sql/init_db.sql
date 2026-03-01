-- ==========================================
-- PostgreSQL Database Schema for ABSA System
-- ==========================================

-- Create database (run separately if needed)
CREATE DATABASE absa_db;

-- Connect to database
-- \c absa_db;

-- ==========================================
-- Table: sentiment_analysis
-- Stores streaming sentiment analysis results
-- ==========================================
CREATE TABLE IF NOT EXISTS sentiment_analysis (
    id SERIAL PRIMARY KEY,
    review TEXT NOT NULL,

-- Predicted sentiments for each aspect
pred_price INTEGER,
pred_shipping INTEGER,
pred_outlook INTEGER,
pred_quality INTEGER,
pred_size INTEGER,
pred_shop_service INTEGER,
pred_general INTEGER,
pred_others INTEGER,

-- Actual sentiments (ground truth if available)
actual_price INTEGER,
actual_shipping INTEGER,
actual_outlook INTEGER,
actual_quality INTEGER,
actual_size INTEGER,
actual_shop_service INTEGER,
actual_general INTEGER,
actual_others INTEGER,

-- Metadata
processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

-- Constraints
CONSTRAINT chk_pred_price CHECK (pred_price BETWEEN -1 AND 2),
    CONSTRAINT chk_pred_shipping CHECK (pred_shipping BETWEEN -1 AND 2),
    CONSTRAINT chk_pred_outlook CHECK (pred_outlook BETWEEN -1 AND 2),
    CONSTRAINT chk_pred_quality CHECK (pred_quality BETWEEN -1 AND 2),
    CONSTRAINT chk_pred_size CHECK (pred_size BETWEEN -1 AND 2),
    CONSTRAINT chk_pred_shop_service CHECK (pred_shop_service BETWEEN -1 AND 2),
    CONSTRAINT chk_pred_general CHECK (pred_general BETWEEN -1 AND 2),
    CONSTRAINT chk_pred_others CHECK (pred_others BETWEEN -1 AND 2)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_processed_at ON sentiment_analysis (processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_pred_price ON sentiment_analysis (pred_price);

CREATE INDEX IF NOT EXISTS idx_pred_quality ON sentiment_analysis (pred_quality);

-- ==========================================
-- Table: model_versions
-- Tracks model versions and performance metrics
-- ==========================================
CREATE TABLE IF NOT EXISTS model_versions (
    id SERIAL PRIMARY KEY,
    version VARCHAR(50) NOT NULL UNIQUE,
    model_path TEXT NOT NULL,

-- Performance metrics
accuracy_price FLOAT,
accuracy_shipping FLOAT,
accuracy_outlook FLOAT,
accuracy_quality FLOAT,
accuracy_size FLOAT,
accuracy_shop_service FLOAT,
accuracy_general FLOAT,
accuracy_others FLOAT,
avg_accuracy FLOAT,
f1_score_price FLOAT,
f1_score_shipping FLOAT,
f1_score_outlook FLOAT,
f1_score_quality FLOAT,
f1_score_size FLOAT,
f1_score_shop_service FLOAT,
f1_score_general FLOAT,
f1_score_others FLOAT,
avg_f1_score FLOAT,

-- Status
is_production BOOLEAN DEFAULT FALSE,

-- Metadata
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
deployed_at TIMESTAMP,
notes TEXT,

-- Ensure only one production model
CONSTRAINT unique_production CHECK (
        is_production = FALSE OR 
        (SELECT COUNT(*) FROM model_versions WHERE is_production = TRUE) <= 1
    )
);

-- Create index on production status
CREATE INDEX IF NOT EXISTS idx_is_production ON model_versions (is_production)
WHERE
    is_production = TRUE;

-- ==========================================
-- Insert initial model version
-- ==========================================
INSERT INTO
    model_versions (
        version,
        model_path,
        is_production,
        notes
    )
VALUES (
        'v1.0_initial',
        'D:\Projects\Assignment\video-counting\models\vietnamese_sentiment_model (1).keras',
        TRUE,
        'Initial production model'
    )
ON CONFLICT (version) DO NOTHING;

-- ==========================================
-- Views for analytics
-- ==========================================

-- View: Recent sentiment statistics
CREATE OR REPLACE VIEW recent_sentiment_stats AS
SELECT DATE_TRUNC('hour', processed_at) as hour, COUNT(*) as total_reviews,

-- Price sentiment distribution
SUM(
    CASE
        WHEN pred_price = -1 THEN 1
        ELSE 0
    END
) as price_none,
SUM(
    CASE
        WHEN pred_price = 0 THEN 1
        ELSE 0
    END
) as price_negative,
SUM(
    CASE
        WHEN pred_price = 1 THEN 1
        ELSE 0
    END
) as price_positive,
SUM(
    CASE
        WHEN pred_price = 2 THEN 1
        ELSE 0
    END
) as price_neutral,

-- Quality sentiment distribution
SUM(
    CASE
        WHEN pred_quality = -1 THEN 1
        ELSE 0
    END
) as quality_none,
SUM(
    CASE
        WHEN pred_quality = 0 THEN 1
        ELSE 0
    END
) as quality_negative,
SUM(
    CASE
        WHEN pred_quality = 1 THEN 1
        ELSE 0
    END
) as quality_positive,
SUM(
    CASE
        WHEN pred_quality = 2 THEN 1
        ELSE 0
    END
) as quality_neutral
FROM sentiment_analysis
WHERE
    processed_at >= NOW() - INTERVAL '24 hours'
GROUP BY
    hour
ORDER BY hour DESC;

-- ==========================================
-- Sample queries for testing
-- ==========================================

-- Get latest 10 reviews
-- SELECT * FROM sentiment_analysis ORDER BY processed_at DESC LIMIT 10;

-- Get sentiment distribution for Price aspect
-- SELECT pred_price, COUNT(*) as count
-- FROM sentiment_analysis
-- GROUP BY pred_price
-- ORDER BY pred_price;

-- Get production model info
-- SELECT * FROM model_versions WHERE is_production = TRUE;