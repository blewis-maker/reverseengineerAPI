-- Add unique constraint to burndown_metrics table
ALTER TABLE burndown_metrics
ADD CONSTRAINT unique_burndown_metrics_utility_date UNIQUE (utility, date); 