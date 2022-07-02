ALTER TABLE staging.user_activity_log;
ADD COLUMN status VARCHAR(30) NOT NULL;
UPDATE staging.user_activity_log SET status = 'shipped';

ALTER TABLE mart.f_sales;
ADD COLUMN status VARCHAR(30) NOT NULL;
DEFAULT 'shipping';
