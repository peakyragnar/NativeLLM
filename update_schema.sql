-- Alter the text fields to use TEXT type instead of VARCHAR(255)
ALTER TABLE financial_data ALTER COLUMN concept TYPE TEXT;
ALTER TABLE financial_data ALTER COLUMN value_text TYPE TEXT;
ALTER TABLE financial_data ALTER COLUMN context_label TYPE TEXT;
ALTER TABLE financial_data ALTER COLUMN company_name TYPE TEXT;

ALTER TABLE text_blocks ALTER COLUMN title TYPE TEXT;
