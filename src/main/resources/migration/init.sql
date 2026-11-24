IF OBJECT_ID('dbo.customer', 'U') IS NULL
CREATE TABLE customer (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(150) NOT NULL,
    tckn VARCHAR(11) NOT NULL UNIQUE
);

IF OBJECT_ID('dbo.items', 'U') IS NULL
CREATE TABLE items (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(150) NOT NULL,
    price DECIMAL(18,2) NOT NULL
);

IF OBJECT_ID('dbo.invoices', 'U') IS NULL
CREATE TABLE invoices (
    id INT IDENTITY(1,1) PRIMARY KEY,
    serial NVARCHAR(20) NOT NULL,
    no NVARCHAR(50) NOT NULL,
    customer_id INT NOT NULL FOREIGN KEY REFERENCES customer(id),
    total_amount DECIMAL(18,2) NOT NULL,
    discount_amount DECIMAL(18,2) NOT NULL,
    payable_amount DECIMAL(18,2) NOT NULL,
    CONSTRAINT UQ_Invoice_Serial_No UNIQUE (serial, no)
);

IF OBJECT_ID('dbo.invoiceItems', 'U') IS NULL
CREATE TABLE invoiceItems (
    id INT IDENTITY(1,1) PRIMARY KEY,
    invoice_id INT NOT NULL FOREIGN KEY REFERENCES invoices(id) ON DELETE CASCADE,
    item_id INT NOT NULL FOREIGN KEY REFERENCES items(id),
    quantity INT NOT NULL,
    total DECIMAL(18,2) NOT NULL
);