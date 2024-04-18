-- Users Table
CREATE TABLE Users (
    user_id VARCHAR(255) PRIMARY KEY,
    avg_stars DECIMAL(3, 2),
    name VARCHAR(255),
    review_count INT,
    yelping_since DATE
);

-- Business Table
CREATE TABLE Business (
    business_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    state CHAR(2), 
    zipcode CHAR(10), 
    stars DECIMAL(2, 1),
    review_rating DECIMAL(2, 1), 
    review_count INT,
    num_checkins INT
    );

-- Review Table
CREATE TABLE Review (
    review_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    business_id VARCHAR(255),
    stars INT CHECK (stars BETWEEN 1 AND 5),
    date DATE,
    text TEXT,
    useful INT,
    funny INT,
    cool INT,
    FOREIGN KEY (user_id) REFERENCES Users(user_id),
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);


-- Checkins Table 
CREATE TABLE Checkins (
    business_id VARCHAR(255) NOT NULL,
    day VARCHAR(9),
    time VARCHAR(8),
    count INT,
    PRIMARY KEY (business_id, day, time),
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

-- Categories Table
CREATE TABLE Categories (
    business_id VARCHAR(255),
    cat_name VARCHAR(255),
    PRIMARY KEY (business_id, cat_name),
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

-- Attributes Table
CREATE TABLE Attributes (
    business_id VARCHAR(255) NOT NULL,
    attr_name VARCHAR(255) NOT NULL,
    value VARCHAR(255),
    PRIMARY KEY (business_id, attr_name),
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

