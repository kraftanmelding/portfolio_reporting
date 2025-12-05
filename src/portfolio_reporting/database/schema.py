"""Database schema definitions for portfolio reporting."""

# SQL schema for creating all tables
SCHEMA_SQL = """
-- Companies table
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(id)
);

-- Power plants table
CREATE TABLE IF NOT EXISTS power_plants (
    id INTEGER PRIMARY KEY,
    uuid TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    company_id INTEGER,
    power_plant_type TEXT,
    capacity_mw REAL,
    latitude REAL,
    longitude REAL,
    commissioned_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (company_id) REFERENCES companies (id)
);

-- Production days table
CREATE TABLE IF NOT EXISTS production_days (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    power_plant_id INTEGER NOT NULL,
    date DATE NOT NULL,
    volume REAL,
    revenue_nok REAL,
    revenue_eur REAL,
    forecasted_volume REAL,
    cap_theoretical_volume REAL,
    full_load_count INTEGER,
    no_load_count INTEGER,
    operational_count INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(power_plant_id, date),
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Production periods table (hourly production)
CREATE TABLE IF NOT EXISTS production_periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    power_plant_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    volume REAL,
    revenue_nok REAL,
    revenue_eur REAL,
    forecasted_volume REAL,
    downtime_volume REAL,
    downtime_cost_nok REAL,
    downtime_cost_eur REAL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(power_plant_id, timestamp),
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Market prices table
CREATE TABLE IF NOT EXISTS market_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price_area TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price REAL,
    currency TEXT DEFAULT 'NOK',
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(price_area, timestamp)
);

-- Downtime events table
CREATE TABLE IF NOT EXISTS downtime_events (
    id INTEGER PRIMARY KEY,
    power_plant_id INTEGER NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_hours REAL,
    reason TEXT,
    event_type TEXT,
    lost_production_kwh REAL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Downtime days table (daily aggregated downtime)
CREATE TABLE IF NOT EXISTS downtime_days (
    id INTEGER PRIMARY KEY,
    power_plant_id INTEGER NOT NULL,
    date DATE NOT NULL,
    reason TEXT,
    volume REAL,
    cost_nok REAL,
    cost_eur REAL,
    hour_count INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(power_plant_id, date, reason),
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Downtime periods table (hourly downtime periods)
CREATE TABLE IF NOT EXISTS downtime_periods (
    id INTEGER PRIMARY KEY,
    power_plant_id INTEGER NOT NULL,
    downtime_event_id INTEGER,
    timestamp TIMESTAMP NOT NULL,
    reason TEXT,
    volume REAL,
    cost_nok REAL,
    cost_eur REAL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(power_plant_id, timestamp),
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Scheduled downtime events table
CREATE TABLE IF NOT EXISTS scheduled_downtime_events (
    id INTEGER PRIMARY KEY,
    power_plant_id INTEGER NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_hours REAL,
    reason TEXT,
    notes TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Work items table (O&M tracking)
CREATE TABLE IF NOT EXISTS work_items (
    id INTEGER PRIMARY KEY,
    power_plant_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT,
    priority TEXT,
    assigned_to TEXT,
    due_date DATE,
    completed_at TIMESTAMP,
    budget_cost_nok REAL,
    budget_cost_eur REAL,
    elapsed_cost_nok REAL,
    elapsed_cost_eur REAL,
    forecast_cost_nok REAL,
    forecast_cost_eur REAL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Budgets table (monthly production budgets)
CREATE TABLE IF NOT EXISTS budgets (
    id INTEGER PRIMARY KEY,
    power_plant_id INTEGER NOT NULL,
    month DATE NOT NULL,
    volume REAL,
    revenue_nok REAL,
    revenue_eur REAL,
    avg_daily_volume REAL,
    avg_daily_revenue_nok REAL,
    avg_daily_revenue_eur REAL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(power_plant_id, month),
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Sensors table (optional, for detailed monitoring)
CREATE TABLE IF NOT EXISTS sensors (
    id INTEGER PRIMARY KEY,
    power_plant_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    sensor_type TEXT,
    unit TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (power_plant_id) REFERENCES power_plants (id)
);

-- Sensor readings table (optional, can be large)
CREATE TABLE IF NOT EXISTS sensor_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value REAL,
    created_at TIMESTAMP,
    UNIQUE(sensor_id, timestamp),
    FOREIGN KEY (sensor_id) REFERENCES sensors (id)
);

-- Sync metadata table (tracks last sync for incremental updates)
CREATE TABLE IF NOT EXISTS sync_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT UNIQUE NOT NULL,
    last_sync_at TIMESTAMP NOT NULL,
    last_sync_success BOOLEAN DEFAULT 1,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_power_plants_company
    ON power_plants(company_id);

CREATE INDEX IF NOT EXISTS idx_power_plants_uuid
    ON power_plants(uuid);

CREATE INDEX IF NOT EXISTS idx_production_days_date
    ON production_days(date);

CREATE INDEX IF NOT EXISTS idx_production_days_power_plant
    ON production_days(power_plant_id, date);

CREATE INDEX IF NOT EXISTS idx_production_periods_power_plant
    ON production_periods(power_plant_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_market_prices_timestamp
    ON market_prices(timestamp);

CREATE INDEX IF NOT EXISTS idx_market_prices_area_time
    ON market_prices(price_area, timestamp);

CREATE INDEX IF NOT EXISTS idx_downtime_events_power_plant
    ON downtime_events(power_plant_id, start_time);

CREATE INDEX IF NOT EXISTS idx_downtime_days_power_plant
    ON downtime_days(power_plant_id, date);

CREATE INDEX IF NOT EXISTS idx_downtime_periods_power_plant
    ON downtime_periods(power_plant_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_work_items_power_plant
    ON work_items(power_plant_id);

CREATE INDEX IF NOT EXISTS idx_work_items_status
    ON work_items(status);

CREATE INDEX IF NOT EXISTS idx_budgets_power_plant
    ON budgets(power_plant_id, month);

CREATE INDEX IF NOT EXISTS idx_sensors_power_plant
    ON sensors(power_plant_id);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_time
    ON sensor_readings(sensor_id, timestamp);
"""
