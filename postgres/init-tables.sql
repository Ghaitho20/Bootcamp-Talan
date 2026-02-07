

-- 1️⃣ Trees by arrondissement
CREATE TABLE IF NOT EXISTS trees_by_arrondissement (
    arrondissement VARCHAR(50),
    total_trees INT,
    timestamp TIMESTAMP
);

-- 2️⃣ Top species
CREATE TABLE IF NOT EXISTS trees_by_species (
    espece VARCHAR(100),
    count INT,
    timestamp TIMESTAMP
);

-- 3️⃣ Height statistics
CREATE TABLE IF NOT EXISTS trees_height_stats (
    espece VARCHAR(100),
    avg_height DOUBLE PRECISION,
    max_height DOUBLE PRECISION,
    min_height DOUBLE PRECISION,
    count INT,
    timestamp TIMESTAMP
);

-- 4️⃣ Remarkable trees per arrondissement
CREATE TABLE IF NOT EXISTS trees_remarkable_stats (
    arrondissement VARCHAR(50),
    total_trees INT,
    remarkable_trees INT,
    timestamp TIMESTAMP
);

-- 5️⃣ Geo data
CREATE TABLE IF NOT EXISTS trees_geo_data (
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    arrondissement VARCHAR(50),
    espece VARCHAR(100),
    hauteur DOUBLE PRECISION,
    remarquable VARCHAR(10),
    timestamp TIMESTAMP
);
