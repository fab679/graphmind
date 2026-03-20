// ============================================================================
// Graphmind Social Network Demo
// A rich social graph: people, marriages, cities, properties, jobs, hobbies
// ============================================================================
// Usage:
//   Run each block sequentially via the Graphmind Visualizer (ui/) or RESP client.
//   CREATE statements don't support inline RETURN — query after creating.
// ============================================================================


// --- CITIES ---

CREATE (c:City {name: 'San Francisco', state: 'California', country: 'USA', population: 874961})
CREATE (c:City {name: 'New York', state: 'New York', country: 'USA', population: 8336817})
CREATE (c:City {name: 'Austin', state: 'Texas', country: 'USA', population: 978908})
CREATE (c:City {name: 'Seattle', state: 'Washington', country: 'USA', population: 737015})
CREATE (c:City {name: 'London', state: 'England', country: 'UK', population: 8982000})
CREATE (c:City {name: 'Tokyo', state: 'Kanto', country: 'Japan', population: 13960000})


// --- PEOPLE ---

CREATE (p:Person {name: 'Alice Chen', age: 34, email: 'alice@example.com', occupation: 'Software Engineer'})
CREATE (p:Person {name: 'Bob Chen', age: 36, email: 'bob@example.com', occupation: 'Product Manager'})
CREATE (p:Person {name: 'Carol Martinez', age: 29, email: 'carol@example.com', occupation: 'Data Scientist'})
CREATE (p:Person {name: 'David Kim', age: 31, email: 'david@example.com', occupation: 'UX Designer'})
CREATE (p:Person {name: 'Eve Johnson', age: 42, email: 'eve@example.com', occupation: 'CTO'})
CREATE (p:Person {name: 'Frank Johnson', age: 44, email: 'frank@example.com', occupation: 'Architect'})
CREATE (p:Person {name: 'Grace Lee', age: 27, email: 'grace@example.com', occupation: 'Marketing Director'})
CREATE (p:Person {name: 'Henry Park', age: 33, email: 'henry@example.com', occupation: 'DevOps Engineer'})
CREATE (p:Person {name: 'Iris Tanaka', age: 38, email: 'iris@example.com', occupation: 'Researcher'})
CREATE (p:Person {name: 'Jack Wilson', age: 45, email: 'jack@example.com', occupation: 'Investor'})
CREATE (p:Person {name: 'Karen Smith', age: 30, email: 'karen@example.com', occupation: 'Lawyer'})
CREATE (p:Person {name: 'Leo Rivera', age: 28, email: 'leo@example.com', occupation: 'Startup Founder'})
CREATE (p:Person {name: 'Mia Patel', age: 35, email: 'mia@example.com', occupation: 'Doctor'})
CREATE (p:Person {name: 'Nathan Patel', age: 37, email: 'nathan@example.com', occupation: 'Professor'})
CREATE (p:Person {name: 'Olivia Brown', age: 26, email: 'olivia@example.com', occupation: 'Journalist'})
CREATE (p:Person {name: 'Paul Wright', age: 50, email: 'paul@example.com', occupation: 'CEO'})


// --- COMPANIES ---

CREATE (c:Company {name: 'TechNova', industry: 'Technology', founded: 2015, employees: 450})
CREATE (c:Company {name: 'HealthFirst', industry: 'Healthcare', founded: 2010, employees: 1200})
CREATE (c:Company {name: 'GreenLeaf Ventures', industry: 'Investment', founded: 2008, employees: 35})
CREATE (c:Company {name: 'MediaPulse', industry: 'Media', founded: 2018, employees: 80})
CREATE (c:Company {name: 'BuildRight', industry: 'Architecture', founded: 2005, employees: 200})


// --- PROPERTIES (houses, cars, pets) ---

CREATE (h:Property {type: 'House', address: '742 Evergreen Terrace', value: 1200000, bedrooms: 4})
CREATE (h:Property {type: 'Condo', address: '55 Central Park West', value: 2500000, bedrooms: 3})
CREATE (h:Property {type: 'House', address: '1600 Amphitheatre Pkwy', value: 950000, bedrooms: 3})
CREATE (h:Property {type: 'Apartment', address: '221B Baker Street', value: 800000, bedrooms: 2})
CREATE (h:Property {type: 'House', address: '10 Downing Lane', value: 1800000, bedrooms: 5})
CREATE (h:Property {type: 'Condo', address: '350 Fifth Avenue', value: 3200000, bedrooms: 2})
CREATE (h:Property {type: 'House', address: '12 Cherry Blossom Rd', value: 650000, bedrooms: 3})

CREATE (c:Car {make: 'Tesla', model: 'Model S', year: 2023, value: 89000})
CREATE (c:Car {make: 'BMW', model: 'X5', year: 2022, value: 65000})
CREATE (c:Car {make: 'Toyota', model: 'Camry', year: 2024, value: 32000})
CREATE (c:Car {make: 'Porsche', model: '911', year: 2023, value: 120000})
CREATE (c:Car {make: 'Honda', model: 'Civic', year: 2021, value: 25000})
CREATE (c:Car {make: 'Mercedes', model: 'E-Class', year: 2023, value: 72000})

CREATE (p:Pet {name: 'Luna', species: 'Dog', breed: 'Golden Retriever', age: 3})
CREATE (p:Pet {name: 'Mochi', species: 'Cat', breed: 'Scottish Fold', age: 5})
CREATE (p:Pet {name: 'Buddy', species: 'Dog', breed: 'Labrador', age: 7})
CREATE (p:Pet {name: 'Whiskers', species: 'Cat', breed: 'Siamese', age: 2})
CREATE (p:Pet {name: 'Koi', species: 'Fish', breed: 'Koi', age: 4})


// --- HOBBIES ---

CREATE (h:Hobby {name: 'Photography', category: 'Creative'})
CREATE (h:Hobby {name: 'Rock Climbing', category: 'Sports'})
CREATE (h:Hobby {name: 'Cooking', category: 'Lifestyle'})
CREATE (h:Hobby {name: 'Piano', category: 'Music'})
CREATE (h:Hobby {name: 'Hiking', category: 'Sports'})
CREATE (h:Hobby {name: 'Reading', category: 'Lifestyle'})
CREATE (h:Hobby {name: 'Gaming', category: 'Entertainment'})
CREATE (h:Hobby {name: 'Painting', category: 'Creative'})


// --- UNIVERSITIES ---

CREATE (u:University {name: 'Stanford', location: 'Palo Alto', ranking: 3})
CREATE (u:University {name: 'MIT', location: 'Cambridge', ranking: 1})
CREATE (u:University {name: 'University of Tokyo', location: 'Tokyo', ranking: 28})
CREATE (u:University {name: 'Oxford', location: 'Oxford', ranking: 4})


// --- MARRIAGES ---

MATCH (a:Person {name: 'Alice Chen'}), (b:Person {name: 'Bob Chen'}) CREATE (a)-[:MARRIED_TO {since: 2018, venue: 'Napa Valley'}]->(b)
MATCH (a:Person {name: 'Eve Johnson'}), (b:Person {name: 'Frank Johnson'}) CREATE (a)-[:MARRIED_TO {since: 2010, venue: 'Cape Cod'}]->(b)
MATCH (a:Person {name: 'Mia Patel'}), (b:Person {name: 'Nathan Patel'}) CREATE (a)-[:MARRIED_TO {since: 2020, venue: 'Mumbai'}]->(b)
MATCH (a:Person {name: 'Carol Martinez'}), (b:Person {name: 'David Kim'}) CREATE (a)-[:MARRIED_TO {since: 2023, venue: 'Austin'}]->(b)


// --- LIVES_IN (people → cities) ---

MATCH (p:Person {name: 'Alice Chen'}), (c:City {name: 'San Francisco'}) CREATE (p)-[:LIVES_IN {since: 2016}]->(c)
MATCH (p:Person {name: 'Bob Chen'}), (c:City {name: 'San Francisco'}) CREATE (p)-[:LIVES_IN {since: 2016}]->(c)
MATCH (p:Person {name: 'Carol Martinez'}), (c:City {name: 'Austin'}) CREATE (p)-[:LIVES_IN {since: 2019}]->(c)
MATCH (p:Person {name: 'David Kim'}), (c:City {name: 'Austin'}) CREATE (p)-[:LIVES_IN {since: 2020}]->(c)
MATCH (p:Person {name: 'Eve Johnson'}), (c:City {name: 'Seattle'}) CREATE (p)-[:LIVES_IN {since: 2012}]->(c)
MATCH (p:Person {name: 'Frank Johnson'}), (c:City {name: 'Seattle'}) CREATE (p)-[:LIVES_IN {since: 2012}]->(c)
MATCH (p:Person {name: 'Grace Lee'}), (c:City {name: 'New York'}) CREATE (p)-[:LIVES_IN {since: 2021}]->(c)
MATCH (p:Person {name: 'Henry Park'}), (c:City {name: 'San Francisco'}) CREATE (p)-[:LIVES_IN {since: 2018}]->(c)
MATCH (p:Person {name: 'Iris Tanaka'}), (c:City {name: 'Tokyo'}) CREATE (p)-[:LIVES_IN {since: 2005}]->(c)
MATCH (p:Person {name: 'Jack Wilson'}), (c:City {name: 'New York'}) CREATE (p)-[:LIVES_IN {since: 2000}]->(c)
MATCH (p:Person {name: 'Karen Smith'}), (c:City {name: 'London'}) CREATE (p)-[:LIVES_IN {since: 2017}]->(c)
MATCH (p:Person {name: 'Leo Rivera'}), (c:City {name: 'Austin'}) CREATE (p)-[:LIVES_IN {since: 2022}]->(c)
MATCH (p:Person {name: 'Mia Patel'}), (c:City {name: 'New York'}) CREATE (p)-[:LIVES_IN {since: 2015}]->(c)
MATCH (p:Person {name: 'Nathan Patel'}), (c:City {name: 'New York'}) CREATE (p)-[:LIVES_IN {since: 2014}]->(c)
MATCH (p:Person {name: 'Olivia Brown'}), (c:City {name: 'London'}) CREATE (p)-[:LIVES_IN {since: 2020}]->(c)
MATCH (p:Person {name: 'Paul Wright'}), (c:City {name: 'Seattle'}) CREATE (p)-[:LIVES_IN {since: 1998}]->(c)


// --- FRIENDSHIPS ---

MATCH (a:Person {name: 'Alice Chen'}), (b:Person {name: 'Carol Martinez'}) CREATE (a)-[:FRIENDS_WITH {since: 2015, how_met: 'college'}]->(b)
MATCH (a:Person {name: 'Alice Chen'}), (b:Person {name: 'Henry Park'}) CREATE (a)-[:FRIENDS_WITH {since: 2017, how_met: 'work'}]->(b)
MATCH (a:Person {name: 'Alice Chen'}), (b:Person {name: 'Grace Lee'}) CREATE (a)-[:FRIENDS_WITH {since: 2019, how_met: 'conference'}]->(b)
MATCH (a:Person {name: 'Bob Chen'}), (b:Person {name: 'David Kim'}) CREATE (a)-[:FRIENDS_WITH {since: 2018, how_met: 'work'}]->(b)
MATCH (a:Person {name: 'Bob Chen'}), (b:Person {name: 'Leo Rivera'}) CREATE (a)-[:FRIENDS_WITH {since: 2021, how_met: 'networking'}]->(b)
MATCH (a:Person {name: 'Carol Martinez'}), (b:Person {name: 'Mia Patel'}) CREATE (a)-[:FRIENDS_WITH {since: 2016, how_met: 'college'}]->(b)
MATCH (a:Person {name: 'Carol Martinez'}), (b:Person {name: 'Olivia Brown'}) CREATE (a)-[:FRIENDS_WITH {since: 2020, how_met: 'online'}]->(b)
MATCH (a:Person {name: 'Eve Johnson'}), (b:Person {name: 'Paul Wright'}) CREATE (a)-[:FRIENDS_WITH {since: 2008, how_met: 'industry'}]->(b)
MATCH (a:Person {name: 'Eve Johnson'}), (b:Person {name: 'Jack Wilson'}) CREATE (a)-[:FRIENDS_WITH {since: 2012, how_met: 'business'}]->(b)
MATCH (a:Person {name: 'Grace Lee'}), (b:Person {name: 'Olivia Brown'}) CREATE (a)-[:FRIENDS_WITH {since: 2021, how_met: 'gym'}]->(b)
MATCH (a:Person {name: 'Grace Lee'}), (b:Person {name: 'Karen Smith'}) CREATE (a)-[:FRIENDS_WITH {since: 2019, how_met: 'book club'}]->(b)
MATCH (a:Person {name: 'Henry Park'}), (b:Person {name: 'Leo Rivera'}) CREATE (a)-[:FRIENDS_WITH {since: 2020, how_met: 'hackathon'}]->(b)
MATCH (a:Person {name: 'Iris Tanaka'}), (b:Person {name: 'Nathan Patel'}) CREATE (a)-[:FRIENDS_WITH {since: 2013, how_met: 'research'}]->(b)
MATCH (a:Person {name: 'Jack Wilson'}), (b:Person {name: 'Paul Wright'}) CREATE (a)-[:FRIENDS_WITH {since: 2005, how_met: 'golf'}]->(b)
MATCH (a:Person {name: 'Karen Smith'}), (b:Person {name: 'Mia Patel'}) CREATE (a)-[:FRIENDS_WITH {since: 2018, how_met: 'neighbors'}]->(b)
MATCH (a:Person {name: 'Leo Rivera'}), (b:Person {name: 'David Kim'}) CREATE (a)-[:FRIENDS_WITH {since: 2022, how_met: 'coworking'}]->(b)
MATCH (a:Person {name: 'Nathan Patel'}), (b:Person {name: 'Paul Wright'}) CREATE (a)-[:FRIENDS_WITH {since: 2010, how_met: 'conference'}]->(b)
MATCH (a:Person {name: 'Olivia Brown'}), (b:Person {name: 'Iris Tanaka'}) CREATE (a)-[:FRIENDS_WITH {since: 2021, how_met: 'interview'}]->(b)


// --- WORKS_AT (people → companies) ---

MATCH (p:Person {name: 'Alice Chen'}), (c:Company {name: 'TechNova'}) CREATE (p)-[:WORKS_AT {role: 'Senior Engineer', since: 2019}]->(c)
MATCH (p:Person {name: 'Bob Chen'}), (c:Company {name: 'TechNova'}) CREATE (p)-[:WORKS_AT {role: 'Product Lead', since: 2020}]->(c)
MATCH (p:Person {name: 'Eve Johnson'}), (c:Company {name: 'TechNova'}) CREATE (p)-[:WORKS_AT {role: 'CTO', since: 2015}]->(c)
MATCH (p:Person {name: 'Henry Park'}), (c:Company {name: 'TechNova'}) CREATE (p)-[:WORKS_AT {role: 'DevOps Lead', since: 2021}]->(c)
MATCH (p:Person {name: 'Mia Patel'}), (c:Company {name: 'HealthFirst'}) CREATE (p)-[:WORKS_AT {role: 'Chief Medical Officer', since: 2018}]->(c)
MATCH (p:Person {name: 'Nathan Patel'}), (c:Company {name: 'HealthFirst'}) CREATE (p)-[:WORKS_AT {role: 'Research Director', since: 2017}]->(c)
MATCH (p:Person {name: 'Jack Wilson'}), (c:Company {name: 'GreenLeaf Ventures'}) CREATE (p)-[:WORKS_AT {role: 'Managing Partner', since: 2008}]->(c)
MATCH (p:Person {name: 'Grace Lee'}), (c:Company {name: 'MediaPulse'}) CREATE (p)-[:WORKS_AT {role: 'Marketing Director', since: 2022}]->(c)
MATCH (p:Person {name: 'Olivia Brown'}), (c:Company {name: 'MediaPulse'}) CREATE (p)-[:WORKS_AT {role: 'Senior Reporter', since: 2021}]->(c)
MATCH (p:Person {name: 'Frank Johnson'}), (c:Company {name: 'BuildRight'}) CREATE (p)-[:WORKS_AT {role: 'Principal Architect', since: 2010}]->(c)
MATCH (p:Person {name: 'Paul Wright'}), (c:Company {name: 'BuildRight'}) CREATE (p)-[:WORKS_AT {role: 'CEO', since: 2005}]->(c)
MATCH (p:Person {name: 'Carol Martinez'}), (c:Company {name: 'TechNova'}) CREATE (p)-[:WORKS_AT {role: 'Data Scientist', since: 2022}]->(c)
MATCH (p:Person {name: 'David Kim'}), (c:Company {name: 'MediaPulse'}) CREATE (p)-[:WORKS_AT {role: 'UX Lead', since: 2023}]->(c)
MATCH (p:Person {name: 'Karen Smith'}), (c:Company {name: 'GreenLeaf Ventures'}) CREATE (p)-[:WORKS_AT {role: 'General Counsel', since: 2019}]->(c)
MATCH (p:Person {name: 'Leo Rivera'}), (c:Company {name: 'TechNova'}) CREATE (p)-[:WORKS_AT {role: 'Founder-in-Residence', since: 2023}]->(c)


// --- OWNS (people → properties, cars, pets) ---

MATCH (p:Person {name: 'Alice Chen'}), (h:Property {address: '742 Evergreen Terrace'}) CREATE (p)-[:OWNS {purchased: 2019}]->(h)
MATCH (p:Person {name: 'Eve Johnson'}), (h:Property {address: '10 Downing Lane'}) CREATE (p)-[:OWNS {purchased: 2014}]->(h)
MATCH (p:Person {name: 'Jack Wilson'}), (h:Property {address: '55 Central Park West'}) CREATE (p)-[:OWNS {purchased: 2010}]->(h)
MATCH (p:Person {name: 'Jack Wilson'}), (h:Property {address: '350 Fifth Avenue'}) CREATE (p)-[:OWNS {purchased: 2018}]->(h)
MATCH (p:Person {name: 'Mia Patel'}), (h:Property {address: '1600 Amphitheatre Pkwy'}) CREATE (p)-[:OWNS {purchased: 2020}]->(h)
MATCH (p:Person {name: 'Karen Smith'}), (h:Property {address: '221B Baker Street'}) CREATE (p)-[:OWNS {purchased: 2019}]->(h)
MATCH (p:Person {name: 'Iris Tanaka'}), (h:Property {address: '12 Cherry Blossom Rd'}) CREATE (p)-[:OWNS {purchased: 2015}]->(h)

MATCH (p:Person {name: 'Alice Chen'}), (c:Car {make: 'Tesla'}) CREATE (p)-[:OWNS]->(c)
MATCH (p:Person {name: 'Bob Chen'}), (c:Car {make: 'BMW'}) CREATE (p)-[:OWNS]->(c)
MATCH (p:Person {name: 'Eve Johnson'}), (c:Car {make: 'Porsche'}) CREATE (p)-[:OWNS]->(c)
MATCH (p:Person {name: 'Frank Johnson'}), (c:Car {make: 'Mercedes'}) CREATE (p)-[:OWNS]->(c)
MATCH (p:Person {name: 'Carol Martinez'}), (c:Car {make: 'Toyota'}) CREATE (p)-[:OWNS]->(c)
MATCH (p:Person {name: 'Leo Rivera'}), (c:Car {make: 'Honda'}) CREATE (p)-[:OWNS]->(c)

MATCH (p:Person {name: 'Alice Chen'}), (pet:Pet {name: 'Luna'}) CREATE (p)-[:OWNS]->(pet)
MATCH (p:Person {name: 'Eve Johnson'}), (pet:Pet {name: 'Buddy'}) CREATE (p)-[:OWNS]->(pet)
MATCH (p:Person {name: 'Iris Tanaka'}), (pet:Pet {name: 'Mochi'}) CREATE (p)-[:OWNS]->(pet)
MATCH (p:Person {name: 'Iris Tanaka'}), (pet:Pet {name: 'Koi'}) CREATE (p)-[:OWNS]->(pet)
MATCH (p:Person {name: 'Grace Lee'}), (pet:Pet {name: 'Whiskers'}) CREATE (p)-[:OWNS]->(pet)


// --- ATTENDED (people → universities) ---

MATCH (p:Person {name: 'Alice Chen'}), (u:University {name: 'Stanford'}) CREATE (p)-[:ATTENDED {degree: 'MS Computer Science', year: 2014}]->(u)
MATCH (p:Person {name: 'Carol Martinez'}), (u:University {name: 'Stanford'}) CREATE (p)-[:ATTENDED {degree: 'MS Data Science', year: 2017}]->(u)
MATCH (p:Person {name: 'Eve Johnson'}), (u:University {name: 'MIT'}) CREATE (p)-[:ATTENDED {degree: 'PhD Computer Science', year: 2006}]->(u)
MATCH (p:Person {name: 'Nathan Patel'}), (u:University {name: 'MIT'}) CREATE (p)-[:ATTENDED {degree: 'PhD Bioengineering', year: 2011}]->(u)
MATCH (p:Person {name: 'Iris Tanaka'}), (u:University {name: 'University of Tokyo'}) CREATE (p)-[:ATTENDED {degree: 'PhD Physics', year: 2010}]->(u)
MATCH (p:Person {name: 'Karen Smith'}), (u:University {name: 'Oxford'}) CREATE (p)-[:ATTENDED {degree: 'JD Law', year: 2016}]->(u)
MATCH (p:Person {name: 'Mia Patel'}), (u:University {name: 'Stanford'}) CREATE (p)-[:ATTENDED {degree: 'MD Medicine', year: 2015}]->(u)
MATCH (p:Person {name: 'Henry Park'}), (u:University {name: 'MIT'}) CREATE (p)-[:ATTENDED {degree: 'BS Computer Science', year: 2013}]->(u)


// --- ENJOYS (people → hobbies) ---

MATCH (p:Person {name: 'Alice Chen'}), (h:Hobby {name: 'Photography'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Alice Chen'}), (h:Hobby {name: 'Hiking'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Bob Chen'}), (h:Hobby {name: 'Cooking'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Bob Chen'}), (h:Hobby {name: 'Gaming'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Carol Martinez'}), (h:Hobby {name: 'Rock Climbing'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Carol Martinez'}), (h:Hobby {name: 'Reading'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'David Kim'}), (h:Hobby {name: 'Painting'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'David Kim'}), (h:Hobby {name: 'Photography'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Eve Johnson'}), (h:Hobby {name: 'Piano'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Frank Johnson'}), (h:Hobby {name: 'Hiking'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Grace Lee'}), (h:Hobby {name: 'Cooking'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Grace Lee'}), (h:Hobby {name: 'Reading'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Henry Park'}), (h:Hobby {name: 'Rock Climbing'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Henry Park'}), (h:Hobby {name: 'Gaming'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Iris Tanaka'}), (h:Hobby {name: 'Piano'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Iris Tanaka'}), (h:Hobby {name: 'Photography'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Jack Wilson'}), (h:Hobby {name: 'Hiking'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Karen Smith'}), (h:Hobby {name: 'Reading'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Leo Rivera'}), (h:Hobby {name: 'Gaming'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Leo Rivera'}), (h:Hobby {name: 'Rock Climbing'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Olivia Brown'}), (h:Hobby {name: 'Photography'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Olivia Brown'}), (h:Hobby {name: 'Painting'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Paul Wright'}), (h:Hobby {name: 'Cooking'}) CREATE (p)-[:ENJOYS]->(h)
MATCH (p:Person {name: 'Mia Patel'}), (h:Hobby {name: 'Hiking'}) CREATE (p)-[:ENJOYS]->(h)


// --- INVESTED_IN (investors → companies) ---

MATCH (p:Person {name: 'Jack Wilson'}), (c:Company {name: 'TechNova'}) CREATE (p)-[:INVESTED_IN {amount: 2000000, round: 'Series A', year: 2016}]->(c)
MATCH (p:Person {name: 'Jack Wilson'}), (c:Company {name: 'MediaPulse'}) CREATE (p)-[:INVESTED_IN {amount: 500000, round: 'Seed', year: 2018}]->(c)
MATCH (p:Person {name: 'Paul Wright'}), (c:Company {name: 'TechNova'}) CREATE (p)-[:INVESTED_IN {amount: 1000000, round: 'Series A', year: 2016}]->(c)
MATCH (p:Person {name: 'Eve Johnson'}), (c:Company {name: 'HealthFirst'}) CREATE (p)-[:INVESTED_IN {amount: 750000, round: 'Series B', year: 2019}]->(c)


// --- COMPANY HQ (companies → cities) ---

MATCH (c:Company {name: 'TechNova'}), (city:City {name: 'San Francisco'}) CREATE (c)-[:HEADQUARTERED_IN]->(city)
MATCH (c:Company {name: 'HealthFirst'}), (city:City {name: 'New York'}) CREATE (c)-[:HEADQUARTERED_IN]->(city)
MATCH (c:Company {name: 'GreenLeaf Ventures'}), (city:City {name: 'New York'}) CREATE (c)-[:HEADQUARTERED_IN]->(city)
MATCH (c:Company {name: 'MediaPulse'}), (city:City {name: 'Austin'}) CREATE (c)-[:HEADQUARTERED_IN]->(city)
MATCH (c:Company {name: 'BuildRight'}), (city:City {name: 'Seattle'}) CREATE (c)-[:HEADQUARTERED_IN]->(city)


// ============================================================================
// QUERIES TO TEST & SHOWCASE
// ============================================================================


// --- Q1: See the full graph ---
MATCH (n) RETURN n

// --- Q2: All people and where they live ---
MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name, p.occupation ORDER BY c.name, p.name

// --- Q3: Married couples ---
MATCH (a:Person)-[m:MARRIED_TO]->(b:Person) RETURN a.name, b.name, m.since, m.venue

// --- Q4: Who lives in San Francisco? ---
MATCH (p:Person)-[:LIVES_IN]->(c:City {name: 'San Francisco'}) RETURN p.name, p.occupation, p.age ORDER BY p.age

// --- Q5: Friends of Alice Chen ---
MATCH (a:Person {name: 'Alice Chen'})-[:FRIENDS_WITH]->(f:Person) RETURN f.name, f.occupation

// --- Q6: Friends-of-friends (2nd degree network) ---
MATCH (a:Person {name: 'Alice Chen'})-[:FRIENDS_WITH]->(b:Person)-[:FRIENDS_WITH]->(c:Person) WHERE c.name <> 'Alice Chen' RETURN DISTINCT c.name, c.occupation

// --- Q7: Who works at TechNova? ---
MATCH (p:Person)-[w:WORKS_AT]->(c:Company {name: 'TechNova'}) RETURN p.name, w.role ORDER BY w.since

// --- Q8: Coworkers — people who work at the same company ---
MATCH (a:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(b:Person) WHERE a.name < b.name RETURN a.name, b.name, c.name ORDER BY c.name

// --- Q9: What does each person own? ---
MATCH (p:Person)-[:OWNS]->(thing) RETURN p.name, labels(thing), thing

// --- Q10: People who share a hobby ---
MATCH (a:Person)-[:ENJOYS]->(h:Hobby)<-[:ENJOYS]-(b:Person) WHERE a.name < b.name RETURN a.name, b.name, h.name ORDER BY h.name

// --- Q11: People count per city ---
MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN c.name AS city, count(p) AS residents ORDER BY residents DESC

// --- Q12: Average age per company ---
MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name AS company, count(p) AS headcount, avg(p.age) AS avg_age ORDER BY headcount DESC

// --- Q13: Stanford alumni network ---
MATCH (p:Person)-[a:ATTENDED]->(u:University {name: 'Stanford'}) RETURN p.name, a.degree, a.year ORDER BY a.year

// --- Q14: Stanford alumni who are also friends ---
MATCH (a:Person)-[:ATTENDED]->(u:University {name: 'Stanford'}), (b:Person)-[:ATTENDED]->(u), (a)-[:FRIENDS_WITH]->(b) RETURN a.name, b.name

// --- Q15: Jack Wilson's investment portfolio ---
MATCH (j:Person {name: 'Jack Wilson'})-[i:INVESTED_IN]->(c:Company) RETURN c.name, c.industry, i.amount, i.round, i.year

// --- Q16: Total investment per company ---
MATCH (p:Person)-[i:INVESTED_IN]->(c:Company) RETURN c.name, sum(i.amount) AS total_invested, count(p) AS num_investors ORDER BY total_invested DESC

// --- Q17: Pet owners and their animals ---
MATCH (p:Person)-[:OWNS]->(pet:Pet) RETURN p.name, pet.name AS pet_name, pet.species, pet.breed

// --- Q18: People with most connections (degree centrality) ---
MATCH (p:Person)-[r]-() RETURN p.name, count(r) AS connections ORDER BY connections DESC LIMIT 5

// --- Q19: People who live in the same city as their company HQ ---
MATCH (p:Person)-[:LIVES_IN]->(c:City)<-[:HEADQUARTERED_IN]-(co:Company)<-[:WORKS_AT]-(p) RETURN p.name, c.name AS city, co.name AS company

// --- Q20: Married couples who share a hobby ---
MATCH (a:Person)-[:MARRIED_TO]->(b:Person), (a)-[:ENJOYS]->(h:Hobby)<-[:ENJOYS]-(b) RETURN a.name, b.name, h.name AS shared_hobby

// --- Q21: Photography enthusiasts and where they live ---
MATCH (p:Person)-[:ENJOYS]->(h:Hobby {name: 'Photography'}), (p)-[:LIVES_IN]->(c:City) RETURN p.name, c.name AS city ORDER BY c.name

// --- Q22: Full profile for Alice Chen ---
MATCH (a:Person {name: 'Alice Chen'}) OPTIONAL MATCH (a)-[:LIVES_IN]->(city:City) OPTIONAL MATCH (a)-[:WORKS_AT]->(company:Company) OPTIONAL MATCH (a)-[:MARRIED_TO]->(spouse:Person) RETURN a.name, a.age, a.occupation, city.name AS city, company.name AS company, spouse.name AS spouse

// --- Q23: Graph stats ---
MATCH (n) RETURN labels(n) AS type, count(n) AS count ORDER BY count DESC

// --- Q24: All relationship types and counts ---
MATCH ()-[r]->() RETURN type(r) AS relationship, count(r) AS count ORDER BY count DESC

// --- Q25: Visualize Alice's entire neighborhood (ego graph) ---
MATCH (a:Person {name: 'Alice Chen'})-[r]-(connected) RETURN a, r, connected
