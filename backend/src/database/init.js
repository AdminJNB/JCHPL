require('dotenv').config();
const { Pool } = require('pg');

const parseBoolean = (value) => {
  if (typeof value !== 'string') {
    return Boolean(value);
  }

  return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
};

const databaseUrl = process.env.DATABASE_URL?.trim();
const connectionTargets = [databaseUrl, process.env.DB_HOST].filter(Boolean).join(' ').toLowerCase();
const hostedDbProviders = ['supabase.co', 'render.com', 'render.internal', 'railway.app', 'railway.internal', 'neon.tech'];
const isHostedPostgres = hostedDbProviders.some((provider) => connectionTargets.includes(provider));
const sslEnabled = process.env.DB_SSL === undefined ? isHostedPostgres : parseBoolean(process.env.DB_SSL);
const rejectUnauthorized = process.env.DB_SSL_REJECT_UNAUTHORIZED === undefined
  ? false
  : parseBoolean(process.env.DB_SSL_REJECT_UNAUTHORIZED);
const sslConfig = sslEnabled ? { rejectUnauthorized } : false;
const connectionBaseConfig = databaseUrl ? {
  connectionString: databaseUrl,
  ssl: sslConfig,
} : {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: sslConfig,
};

const schema = `
-- Create database
-- DROP DATABASE IF EXISTS jchpl_mis;
-- CREATE DATABASE jchpl_mis;

-- Users table for authentication
CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username VARCHAR(100) UNIQUE NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  name VARCHAR(255),
  mobile VARCHAR(20),
  is_active BOOLEAN DEFAULT true,
  can_delete BOOLEAN DEFAULT false,
  reset_token VARCHAR(255),
  reset_token_expires TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audit log table
CREATE TABLE IF NOT EXISTS audit_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  table_name VARCHAR(100) NOT NULL,
  record_id UUID NOT NULL,
  action VARCHAR(50) NOT NULL,
  old_values JSONB,
  new_values JSONB,
  user_id UUID REFERENCES users(id),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Client Name Master
CREATE TABLE IF NOT EXISTS clients (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Remove legacy parent-client hierarchy support
ALTER TABLE clients DROP COLUMN IF EXISTS parent_id;

-- Billing Name Master
CREATE TABLE IF NOT EXISTS billing_names (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  pan VARCHAR(10),
  client_id UUID REFERENCES clients(id),
  gstin VARCHAR(15),
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Service Type Master
CREATE TABLE IF NOT EXISTS service_types (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  hsn_code VARCHAR(20),
  gst_rate DECIMAL(5, 2) DEFAULT 18.00,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Service Period Master
CREATE TABLE IF NOT EXISTS service_periods (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  period_code VARCHAR(10) NOT NULL UNIQUE,
  display_name VARCHAR(20) NOT NULL,
  financial_year VARCHAR(10) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Team Master
CREATE TABLE IF NOT EXISTS teams (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  mobile VARCHAR(20),
  email VARCHAR(255),
  is_reviewer BOOLEAN DEFAULT false,
  expense_type VARCHAR(20) DEFAULT 'non-recurring',
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Team Client Allocation
CREATE TABLE IF NOT EXISTS team_client_allocations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  team_id UUID REFERENCES teams(id) ON DELETE CASCADE,
  client_id UUID REFERENCES clients(id) ON DELETE CASCADE,
  allocation_percentage DECIMAL(5, 2) NOT NULL,
  start_period VARCHAR(10),
  end_period VARCHAR(10),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reviewer Master (view of teams where is_reviewer = true)
CREATE OR REPLACE VIEW reviewers AS
SELECT id, name, mobile, email, created_at, updated_at
FROM teams WHERE is_reviewer = true AND is_active = true;

-- Expense Head Master
CREATE TABLE IF NOT EXISTS expense_heads (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  is_recurring BOOLEAN DEFAULT false,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Revenue Module
CREATE TABLE IF NOT EXISTS revenues (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  unique_key VARCHAR(500) NOT NULL,
  client_id UUID REFERENCES clients(id),
  service_type_id UUID REFERENCES service_types(id),
  service_period_id UUID REFERENCES service_periods(id),
  date DATE NOT NULL,
  invoice_no VARCHAR(100),
  is_unbilled BOOLEAN DEFAULT false,
  billing_name_id UUID REFERENCES billing_names(id),
  nature_of_service TEXT,
  hsn_code VARCHAR(20),
  gst_rate DECIMAL(5, 2) DEFAULT 18.00,
  revenue_amount DECIMAL(15, 2) NOT NULL,
  currency VARCHAR(3) DEFAULT 'INR',
  igst DECIMAL(15, 2) DEFAULT 0,
  cgst DECIMAL(15, 2) DEFAULT 0,
  sgst DECIMAL(15, 2) DEFAULT 0,
  other_charges DECIMAL(15, 2) DEFAULT 0,
  round_off DECIMAL(10, 2) DEFAULT 0,
  total_amount DECIMAL(15, 2) NOT NULL,
  reviewer_id UUID REFERENCES teams(id),
  reviewer_allocation_percentage DECIMAL(5, 2),
  bill_from VARCHAR(255),
  notes TEXT,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_by UUID REFERENCES users(id),
  updated_by UUID REFERENCES users(id)
);

-- Revenue Reviewer Allocations (for multiple reviewers)
CREATE TABLE IF NOT EXISTS revenue_reviewer_allocations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  revenue_id UUID REFERENCES revenues(id) ON DELETE CASCADE,
  reviewer_id UUID REFERENCES teams(id),
  allocation_percentage DECIMAL(5, 2) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Expense Module
CREATE TABLE IF NOT EXISTS expenses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  unique_key VARCHAR(500) NOT NULL,
  client_id UUID REFERENCES clients(id),
  service_type_id UUID REFERENCES service_types(id),
  service_period_id UUID REFERENCES service_periods(id),
  date DATE NOT NULL,
  is_entered_in_books BOOLEAN DEFAULT false,
  is_unbilled BOOLEAN DEFAULT false,
  ledger_name VARCHAR(255),
  team_id UUID REFERENCES teams(id),
  expense_head_id UUID REFERENCES expense_heads(id),
  description TEXT,
  amount DECIMAL(15, 2) NOT NULL,
  gst_rate DECIMAL(5, 2) DEFAULT 0,
  igst DECIMAL(15, 2) DEFAULT 0,
  cgst DECIMAL(15, 2) DEFAULT 0,
  sgst DECIMAL(15, 2) DEFAULT 0,
  other_charges DECIMAL(15, 2) DEFAULT 0,
  round_off DECIMAL(10, 2) DEFAULT 0,
  total_amount DECIMAL(15, 2) NOT NULL,
  reviewer_id UUID REFERENCES teams(id),
  bill_from VARCHAR(255),
  document_path VARCHAR(500),
  notes TEXT,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_by UUID REFERENCES users(id),
  updated_by UUID REFERENCES users(id)
);

-- Fee Master
CREATE TABLE IF NOT EXISTS fee_masters (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  unique_key VARCHAR(500) NOT NULL UNIQUE,
  billing_name_id UUID REFERENCES billing_names(id),
  client_id UUID REFERENCES clients(id) NOT NULL,
  service_type_id UUID REFERENCES service_types(id) NOT NULL,
  start_period VARCHAR(10) NOT NULL,
  end_period VARCHAR(10),
  fee_amount DECIMAL(15, 2) NOT NULL,
  currency VARCHAR(3) DEFAULT 'INR',
  bill_from VARCHAR(255),
  is_active BOOLEAN DEFAULT true,
  is_ended BOOLEAN DEFAULT false,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_by UUID REFERENCES users(id),
  updated_by UUID REFERENCES users(id)
);

-- Fee Reviewer Allocations
CREATE TABLE IF NOT EXISTS fee_reviewer_allocations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fee_master_id UUID REFERENCES fee_masters(id) ON DELETE CASCADE,
  reviewer_id UUID REFERENCES teams(id),
  allocation_percentage DECIMAL(5, 2),
  allocation_amount DECIMAL(15, 2),
  allocation_method VARCHAR(20) DEFAULT 'percentage',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for fee_masters
CREATE INDEX IF NOT EXISTS idx_fee_masters_client ON fee_masters(client_id);
CREATE INDEX IF NOT EXISTS idx_fee_masters_service_type ON fee_masters(service_type_id);
CREATE INDEX IF NOT EXISTS idx_fee_masters_unique_key ON fee_masters(unique_key);
CREATE INDEX IF NOT EXISTS idx_fee_masters_periods ON fee_masters(start_period, end_period);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_revenues_client ON revenues(client_id);
CREATE INDEX IF NOT EXISTS idx_revenues_service_type ON revenues(service_type_id);
CREATE INDEX IF NOT EXISTS idx_revenues_service_period ON revenues(service_period_id);
CREATE INDEX IF NOT EXISTS idx_revenues_unique_key ON revenues(unique_key);
CREATE INDEX IF NOT EXISTS idx_revenues_date ON revenues(date);

CREATE INDEX IF NOT EXISTS idx_expenses_client ON expenses(client_id);
CREATE INDEX IF NOT EXISTS idx_expenses_service_type ON expenses(service_type_id);
CREATE INDEX IF NOT EXISTS idx_expenses_service_period ON expenses(service_period_id);
CREATE INDEX IF NOT EXISTS idx_expenses_unique_key ON expenses(unique_key);
CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date);

CREATE INDEX IF NOT EXISTS idx_audit_logs_table ON audit_logs(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_logs_record ON audit_logs(record_id);

-- Trigger function for updating timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    -- Only update updated_at if the column exists
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = TG_TABLE_SCHEMA 
        AND table_name = TG_TABLE_NAME 
        AND column_name = 'updated_at'
    ) THEN
        NEW.updated_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply update trigger to all relevant tables
DO $$
DECLARE
    t text;
BEGIN
    FOR t IN SELECT table_name FROM information_schema.tables 
             WHERE table_schema = 'public' 
             AND table_type = 'BASE TABLE'
             AND table_name NOT IN ('audit_logs')
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS update_%s_updated_at ON %s', t, t);
        EXECUTE format('CREATE TRIGGER update_%s_updated_at BEFORE UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()', t, t);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Update admin password to Mahaveer@123 and ensure can_delete
UPDATE users
SET password_hash = '$2a$10$swP/vHfFm.P4tq5MzWdioOPiBh0s1FNU4SqfkhjGi6zYZb7QChSe2',
    can_delete = true
WHERE username = 'admin';

-- Insert default admin user (password: Mahaveer@123)
INSERT INTO users (username, email, password_hash, name, mobile, can_delete)
VALUES ('admin', 'admin@jchpl-mis.com', '$2a$10$swP/vHfFm.P4tq5MzWdioOPiBh0s1FNU4SqfkhjGi6zYZb7QChSe2', 'Administrator', '9999999999', true)
ON CONFLICT (username) DO NOTHING;

-- Insert Kavya user (password: Jchpl@123, no delete permission)
INSERT INTO users (username, email, password_hash, name, mobile, can_delete)
VALUES ('Kavya', 'kavya@jchpl-mis.com', '$2a$10$OKw2245jzj1sKK9UeZXQ6uHap72cUcmoqT7UujI7chZMRSUh0gOke', 'Kavya', '9999999990', false)
ON CONFLICT (username) DO NOTHING;

-- Generate service periods (Mar-26 to Mar-30)
INSERT INTO service_periods (period_code, display_name, financial_year, start_date, end_date)
SELECT 
    TO_CHAR(date_series, 'Mon') || '-' || TO_CHAR(date_series, 'YY') as period_code,
    TO_CHAR(date_series, 'Mon-YY') as display_name,
    CASE 
        WHEN EXTRACT(MONTH FROM date_series) >= 4 
        THEN EXTRACT(YEAR FROM date_series)::TEXT || '-' || (EXTRACT(YEAR FROM date_series) + 1)::TEXT
        ELSE (EXTRACT(YEAR FROM date_series) - 1)::TEXT || '-' || EXTRACT(YEAR FROM date_series)::TEXT
    END as financial_year,
    date_trunc('month', date_series)::DATE as start_date,
    (date_trunc('month', date_series) + INTERVAL '1 month' - INTERVAL '1 day')::DATE as end_date
FROM generate_series('2026-03-01'::DATE, '2030-03-01'::DATE, '1 month'::INTERVAL) as date_series
ON CONFLICT (period_code) DO NOTHING;
`;

async function initDatabase() {
  if (databaseUrl) {
    const appPool = new Pool(connectionBaseConfig);
    const appClient = await appPool.connect();

    try {
      await appClient.query(schema);
      console.log('Database schema initialized successfully via DATABASE_URL');
      return;
    } catch (error) {
      console.error('Error initializing database:', error);
      throw error;
    } finally {
      appClient.release();
      await appPool.end();
    }
  }

  const adminPool = new Pool({
    ...connectionBaseConfig,
    database: 'postgres',
  });
  const client = await adminPool.connect();

  try {
    // Create database if not exists
    const dbCheck = await client.query(
      "SELECT 1 FROM pg_database WHERE datname = 'jchpl_mis'"
    );

    if (dbCheck.rows.length === 0) {
      await client.query('CREATE DATABASE jchpl_mis');
      console.log('Database jchpl_mis created successfully');
    }

    // Connect to the new database
    const appPool = new Pool({
      ...connectionBaseConfig,
      database: process.env.DB_NAME,
    });

    const appClient = await appPool.connect();

    // Run schema
    await appClient.query(schema);
    console.log('Database schema initialized successfully');

    appClient.release();
    await appPool.end();

  } catch (error) {
    console.error('Error initializing database:', error);
    throw error;
  } finally {
    client.release();
    await adminPool.end();
  }
}

initDatabase()
  .then(() => {
    console.log('Database initialization complete');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Database initialization failed:', error);
    process.exit(1);
  });
