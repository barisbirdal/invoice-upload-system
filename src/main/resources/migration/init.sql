CREATE TABLE IF NOT EXISTS customer (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    tckn VARCHAR(11) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS items (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    price NUMERIC(18,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS invoices (
    id SERIAL PRIMARY KEY,
    serial VARCHAR(20) NOT NULL,
    no VARCHAR(50) NOT NULL,
    customer_id INT NOT NULL REFERENCES customer(id),
    total_amount NUMERIC(18,2) NOT NULL,
    discount_amount NUMERIC(18,2) NOT NULL,
    payable_amount NUMERIC(18,2) NOT NULL,
    CONSTRAINT uq_invoice_serial_no UNIQUE (serial, no)
);

CREATE TABLE IF NOT EXISTS invoice_items (
    id SERIAL PRIMARY KEY,
    invoice_id INT NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    item_id INT NOT NULL REFERENCES items(id),
    quantity INT NOT NULL,
    total NUMERIC(18,2) NOT NULL
);