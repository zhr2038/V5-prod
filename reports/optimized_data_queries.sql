-- 优化数据查询脚本
-- 生成时间: 2026-02-18
-- 用于数据质量分析和优化

-- 数据完整性检查

            -- 检查数据完整性
            SELECT 
                symbol,
                COUNT(*) as record_count,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                (MAX(timestamp)-MIN(timestamp))/3600 as hours_range,
                ROUND(COUNT(*)*100.0/720, 2) as coverage_30d_pct,
                CASE 
                    WHEN COUNT(*) >= 700 THEN '优秀'
                    WHEN COUNT(*) >= 650 THEN '良好'
                    WHEN COUNT(*) >= 600 THEN '一般'
                    ELSE '需改进'
                END as quality
            FROM market_data_1h 
            GROUP BY symbol 
            ORDER BY coverage_30d_pct DESC
        

-- 数据质量问题检测

            -- 检测数据质量问题
            WITH issues AS (
                SELECT 
                    symbol,
                    SUM(CASE WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 1 ELSE 0 END) as invalid_prices,
                    SUM(CASE WHEN volume < 0 THEN 1 ELSE 0 END) as negative_volume,
                    SUM(CASE WHEN high < low THEN 1 ELSE 0 END) as high_low_inverted,
                    SUM(CASE WHEN close < low OR close > high THEN 1 ELSE 0 END) as close_out_of_range
                FROM market_data_1h 
                GROUP BY symbol
            )
            SELECT 
                symbol,
                invalid_prices,
                negative_volume,
                high_low_inverted,
                close_out_of_range,
                CASE 
                    WHEN invalid_prices + negative_volume + high_low_inverted + close_out_of_range = 0 THEN '✅ 优秀'
                    ELSE '⚠️ 需检查'
                END as quality_status
            FROM issues
            ORDER BY quality_status, symbol
        

-- 时间连续性分析

            -- 分析时间连续性
            WITH time_gaps AS (
                SELECT 
                    symbol,
                    timestamp,
                    timestamp - LAG(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) as gap_seconds
                FROM market_data_1h
            ),
            gap_stats AS (
                SELECT 
                    symbol,
                    COUNT(*) as total_gaps,
                    SUM(CASE WHEN gap_seconds > 3600 THEN 1 ELSE 0 END) as large_gaps,
                    MAX(gap_seconds) as max_gap_seconds,
                    AVG(gap_seconds) as avg_gap_seconds
                FROM time_gaps 
                WHERE gap_seconds IS NOT NULL
                GROUP BY symbol
            )
            SELECT 
                symbol,
                total_gaps,
                large_gaps,
                max_gap_seconds,
                ROUND(avg_gap_seconds, 0) as avg_gap_seconds,
                CASE 
                    WHEN large_gaps = 0 THEN '✅ 连续'
                    WHEN large_gaps <= 5 THEN '⚠️ 少量缺口'
                    ELSE '❌ 需优化'
                END as continuity_status
            FROM gap_stats
            ORDER BY large_gaps DESC, symbol
        

