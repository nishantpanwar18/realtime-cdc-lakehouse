-- Shipment tracking database for a courier company
-- This DB is UPDATE-HEAVY: shipments move through multiple statuses

-- Grant Debezium the privileges it needs for CDC
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

CREATE DATABASE IF NOT EXISTS shipments_db;
USE shipments_db;

-- Core shipments table
CREATE TABLE shipments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    tracking_id VARCHAR(20) NOT NULL UNIQUE,
    shipper_name VARCHAR(100) NOT NULL,
    shipper_city VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    customer_city VARCHAR(50) NOT NULL,
    weight_kg DECIMAL(6,2) NOT NULL,
    status ENUM(
        'PICKED_UP',
        'IN_TRANSIT',
        'AT_HUB',
        'OUT_FOR_DELIVERY',
        'DELIVERED',
        'RETURNED',
        'FAILED_DELIVERY'
    ) NOT NULL DEFAULT 'PICKED_UP',
    current_location VARCHAR(100),
    estimated_delivery DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_status (status),
    INDEX idx_tracking (tracking_id)
) ENGINE=InnoDB;

-- Seed some initial shipments
INSERT INTO shipments (tracking_id, shipper_name, shipper_city, customer_name, customer_city, weight_kg, status, current_location, estimated_delivery) VALUES
('SHP-10001', 'Acme Electronics', 'Mumbai', 'Rahul Sharma', 'Delhi', 2.50, 'PICKED_UP', 'Mumbai Warehouse', DATE_ADD(CURDATE(), INTERVAL 3 DAY)),
('SHP-10002', 'Fresh Foods Co', 'Bangalore', 'Priya Patel', 'Chennai', 5.00, 'IN_TRANSIT', 'Bangalore Hub', DATE_ADD(CURDATE(), INTERVAL 2 DAY)),
('SHP-10003', 'BookWorld', 'Delhi', 'Amit Kumar', 'Kolkata', 1.20, 'AT_HUB', 'Kolkata Hub', DATE_ADD(CURDATE(), INTERVAL 1 DAY)),
('SHP-10004', 'StyleStore', 'Pune', 'Sneha Reddy', 'Hyderabad', 0.80, 'OUT_FOR_DELIVERY', 'Hyderabad Local', CURDATE()),
('SHP-10005', 'TechGadgets', 'Chennai', 'Vikram Singh', 'Mumbai', 3.30, 'DELIVERED', 'Mumbai - Delivered', CURDATE()),
('SHP-10006', 'HomeDecor Plus', 'Hyderabad', 'Anita Desai', 'Pune', 12.00, 'IN_TRANSIT', 'Solapur Transit', DATE_ADD(CURDATE(), INTERVAL 2 DAY)),
('SHP-10007', 'PharmaCare', 'Kolkata', 'Rajesh Gupta', 'Bangalore', 0.50, 'PICKED_UP', 'Kolkata Warehouse', DATE_ADD(CURDATE(), INTERVAL 4 DAY)),
('SHP-10008', 'AutoParts Hub', 'Jaipur', 'Suresh Yadav', 'Delhi', 8.50, 'AT_HUB', 'Delhi Hub', DATE_ADD(CURDATE(), INTERVAL 1 DAY)),
('SHP-10009', 'GreenGrocers', 'Lucknow', 'Meera Nair', 'Bangalore', 4.00, 'IN_TRANSIT', 'Nagpur Transit', DATE_ADD(CURDATE(), INTERVAL 3 DAY)),
('SHP-10010', 'FashionFirst', 'Mumbai', 'Deepak Joshi', 'Jaipur', 1.50, 'PICKED_UP', 'Mumbai Warehouse', DATE_ADD(CURDATE(), INTERVAL 3 DAY));
