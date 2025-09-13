-- 🎵 Music Industry AI Analysis - Following Colleague's Pattern
-- Demonstrates AI_EXTRACT, AI_CLASSIFY, AI_SENTIMENT, AI_AGG on music data

-- =============================================================================
-- SETUP: Create stage for music documents
-- =============================================================================

create or replace stage MUSIC_DOCS 
	DIRECTORY = ( ENABLE = true 
                  AUTO_REFRESH = TRUE) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

-- Load documents into Snowflake stage
-- MY_STAGE = 'MUSIC_DOCS/press_releases'
-- MY_FILE_NAME = "data/music/press_releases/*.pdf"
-- put_result = session.file.put(MY_FILE_NAME, MY_STAGE, auto_compress=False, overwrite=True)

-- MY_STAGE = 'MUSIC_DOCS/artist_bios'  
-- MY_FILE_NAME = "data/music/artist_bios/*.pdf"
-- put_result = session.file.put(MY_FILE_NAME, MY_STAGE, auto_compress=False, overwrite=True)

alter stage MUSIC_DOCS refresh;

-- =============================================================================
-- AI_EXTRACT: Single document analysis (like machine reports example)
-- =============================================================================

-- AI_EXTRACT leverages Snowflake's Vision model Arctic-extract. 
-- We can ask questions directly on documents without OCR.

SELECT AI_EXTRACT(
  file => TO_FILE('@MUSIC_DOCS/press_releases','billy_strings_tour_2024.pdf'),
  responseFormat => [
    ['artist', 'Who is the main artist mentioned?'], 
    ['tour_dates', 'List: What tour dates are mentioned?']
  ]
) as json_data;

-- We can even use it for classification
SELECT AI_EXTRACT(
  file => TO_FILE('@MUSIC_DOCS/press_releases','billy_strings_tour_2024.pdf'),
  responseFormat => [['genre', 'Is this bluegrass, country, folk, or rock? Answer with one genre']]
) as json_data;

-- =============================================================================
-- Batch processing multiple documents (like machine reports batch)
-- =============================================================================

SELECT 
relative_path,
  json_data:response.artist::STRING as artist_name,
  json_data:response.genre::STRING as genre,
  json_data:response.venue_type::STRING as venue_type,
  json_data:response.tour_info::STRING as tour_info
from
(
SELECT 
relative_path,
AI_EXTRACT(
  file => TO_FILE('@MUSIC_DOCS',RELATIVE_PATH),
  responseFormat => [
    ['artist', 'Who is the main artist mentioned?'], 
    ['genre', 'What music genre is this?'],
    ['venue_type', 'What type of venue (festival, theater, amphitheater)?'],
    ['tour_info', 'What tour or performance information is provided?']
  ]
) as json_data
from DIRECTORY(@MUSIC_DOCS)
where relative_path like 'press_releases/%'
);

-- =============================================================================
-- Extract key information from artist biography (like resume example)
-- =============================================================================

SELECT 
  json_data:response.name::STRING as artist_name,
  json_data:response.birthplace::STRING as birthplace,
  json_data:response.instruments::STRING as instruments,
  json_data:response.awards::STRING as awards
from
(
SELECT AI_EXTRACT(
  file => TO_FILE('@MUSIC_DOCS/artist_bios','sarah_jarosz_bio.pdf'),
  responseFormat => [
    ['name', 'What is the artist full name?'], 
    ['birthplace', 'Where was the artist born?'],
    ['instruments', 'List: What instruments does the artist play?'],
    ['awards', 'List: What awards has the artist won?']
  ]
) as json_data);

-- =============================================================================
-- Parse text from documents using PARSE_DOCUMENT (like resume parsing)
-- =============================================================================

SELECT 
relative_path,
SNOWFLAKE.CORTEX.PARSE_DOCUMENT('@MUSIC_DOCS',relative_path):content::STRING AS bio_text
from DIRECTORY(@MUSIC_DOCS)
where relative_path like 'artist_bios/%'
limit 5;

-- =============================================================================
-- Combine PARSE_DOCUMENT and AI_EXTRACT to create artists table (like candidates)
-- =============================================================================

create or replace table artists as 
SELECT 
  json_data:response.name::STRING as artist_name,
  json_data:response.genre::STRING as primary_genre,
  json_data:response.birthplace::STRING as birthplace,
  json_data:response.instruments::STRING as instruments,
  json_data:response.awards::STRING as awards,
  bio_text,
  relative_path as bio_file_path
from
(
SELECT 
    relative_path,
    
    AI_EXTRACT(
        file => TO_FILE('@MUSIC_DOCS',relative_path),
        responseFormat => [
          ['name', 'What is the artist name?'], 
          ['genre', 'What is their primary music genre?'],
          ['birthplace', 'Where are they from?'],
          ['instruments', 'List: What instruments do they play?'],
          ['awards', 'List: What awards have they won?']
        ]
    ) as json_data,
    
    SNOWFLAKE.CORTEX.PARSE_DOCUMENT('@MUSIC_DOCS',relative_path):content::STRING AS bio_text

from DIRECTORY(@MUSIC_DOCS)
where relative_path like 'artist_bios/%');

select * from artists;

-- =============================================================================
-- Create Music Venues table (like Jobs table)
-- =============================================================================

-- Create table
CREATE OR REPLACE TABLE MUSIC_VENUES (
  venue_id INTEGER AUTOINCREMENT,
  venue_name VARCHAR,
  venue_type VARCHAR,
  location VARCHAR,
  capacity INTEGER,
  description VARCHAR,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (venue_id)
);

-- Seed data with iconic music venues (like job descriptions)
INSERT INTO MUSIC_VENUES (venue_name, venue_type, location, capacity, description) VALUES
  ('Red Rocks Amphitheatre', 'amphitheater', 'Morrison, Colorado', 9525,
   'Iconic outdoor venue carved between red sandstone formations. Known for incredible acoustics and stunning natural beauty. Hosts major touring acts across all genres with a focus on jam bands and folk artists.'),
  ('Ryman Auditorium', 'historic theater', 'Nashville, Tennessee', 2362,
   'The Mother Church of Country Music. Former home of Grand Ole Opry with perfect acoustics. Historic venue where legends like Hank Williams and Patsy Cline performed. Ideal for country, folk, and Americana acts.'),
  ('Telluride Bluegrass Festival', 'festival', 'Telluride, Colorado', 10000,
   'Premier bluegrass festival in stunning mountain setting. Features traditional and progressive bluegrass acts. Known for late-night jam sessions and workshops. Perfect for acoustic string band music.'),
  ('Austin City Limits Live', 'theater', 'Austin, Texas', 2750,
   'Modern venue based on iconic TV show. Features diverse lineup from country to indie rock. State-of-the-art sound and intimate setting in music capital. Great for singer-songwriters and Americana artists.'),
  ('Newport Folk Festival', 'festival', 'Newport, Rhode Island', 10000,
   'Historic folk festival where Bob Dylan went electric. Showcases traditional and contemporary folk artists. Known for surprise collaborations and emerging talent discovery.'),
  ('The Station Inn', 'honky-tonk', 'Nashville, Tennessee', 200,
   'Intimate bluegrass venue where legends jam. No-frills atmosphere focused purely on music. Tuesday night bluegrass sessions are legendary among musicians. Perfect for traditional acoustic music.'),
  ('MerleFest', 'festival', 'Wilkesboro, North Carolina', 80000,
   'Traditional music festival honoring Merle Watson. Features bluegrass, folk, and Americana acts. Multiple stages and educational workshops for all ages.'),
  ('Bonnaroo Music Festival', 'festival', 'Manchester, Tennessee', 80000,
   'Multi-genre festival on 700-acre farm. Features headliners across rock, country, electronic, and hip-hop. Known for late-night shows and camping experience.');

-- =============================================================================
-- AI_FILTER: Match artists to venues (like job matching)
-- =============================================================================

SELECT venue_name, venue_type, location, capacity, description, 
       bio_text, artist_name, primary_genre, instruments
FROM artists a
JOIN MUSIC_VENUES v
ON AI_FILTER(
  PROMPT('Would the artist described in this bio: {0} be a good fit for this venue: {1}? Consider genre, venue size, and atmosphere.', 
         a.bio_text, 
         v.description)
);

-- =============================================================================
-- AI_CLASSIFY: Categorize artists by career stage (like expertise levels)
-- =============================================================================

Select *,
    AI_CLASSIFY(
        bio_text, 
        ['emerging artist', 'established artist', 'legendary artist'],
        {
            'task_description':'Categorize the artist based on their career achievements and recognition'
        }
    ):labels[0]::string as career_stage
from artists;

-- =============================================================================
-- AI_AGG: Analyze trends across documents (like machine reports analysis)
-- =============================================================================

with parsed_press_releases as (
SELECT 
SNOWFLAKE.CORTEX.PARSE_DOCUMENT('@MUSIC_DOCS',relative_path):content::STRING AS press_text
from DIRECTORY(@MUSIC_DOCS)
where relative_path like 'press_releases/%'
)
Select 
    AI_AGG(press_text, 'What are the main trends in the music industry based on these press releases? Focus on venues, touring patterns, and artist collaborations.')
from parsed_press_releases;

-- =============================================================================
-- AI_AGG: Extract common themes by career stage (like skills by expertise)
-- =============================================================================

create or replace temporary table career_stages as
Select bio_text,
    AI_CLASSIFY(
        bio_text, 
        ['emerging artist', 'established artist', 'legendary artist'],
        {
            'task_description':'Categorize the artist based on their career achievements'
        }
    ):labels[0]::string as career_stage
from artists;

SELECT career_stage,
       AI_AGG(bio_text, 'What are common characteristics and career patterns for artists at this stage? List key themes and skills.') AS stage_analysis
  FROM career_stages
 GROUP BY career_stage;

-- =============================================================================
-- AI_SENTIMENT: Music Review Analysis (like movie reviews)
-- =============================================================================

-- Create music reviews table (like movie reviews)
CREATE OR REPLACE TABLE MUSIC_REVIEWS (
    review_id INTEGER AUTOINCREMENT,
    artist VARCHAR,
    album VARCHAR,
    review VARCHAR,
    PRIMARY KEY (review_id)
);

-- Insert sample music reviews
INSERT INTO MUSIC_REVIEWS (artist, album, review) VALUES
  ('Billy Strings', 'Me/And/Dad', 
   'Billy Strings delivers a deeply personal and technically masterful album that honors his roots while pushing bluegrass into new territories. The guitar work is exceptional, though some tracks feel overly sentimental.'),
  ('Sarah Jarosz', 'Polaroid Lovers', 
   'Sarah Jarosz has crafted her most mature and emotionally resonant work yet. The fingerpicking is exquisite and the songwriting is vulnerable and honest. A stunning achievement in contemporary folk.'),
  ('Sturgill Simpson', 'The Ballad of Dood and Juanita', 
   'Sturgill Simpson strips away the psychedelic elements for raw, acoustic storytelling. The concept is ambitious and mostly successful, though the pacing drags in the middle section.'),
  ('Kitchen Dwellers', 'Seven Devils', 
   'Kitchen Dwellers blend traditional bluegrass with modern sensibilities perfectly. The energy is infectious and the musicianship is tight, but the production could be cleaner.'),
  ('Paul Cauthen', 'Country Coming Down', 
   'Paul Cauthen brings theatrical flair and genuine emotion to Texas country. His voice is powerful and the songs are well-crafted, though the album feels slightly repetitive by the end.');

select * from MUSIC_REVIEWS;

-- Basic sentiment analysis
SELECT
  AI_SENTIMENT(
    review
  ),
  artist,
  album,
  review
  FROM MUSIC_REVIEWS;

-- Multi-aspect sentiment analysis (like movie review aspects)
SELECT
  AI_SENTIMENT(
    review,
    ['musicianship', 'songwriting', 'production', 'vocals', 'innovation']
  ),
  artist,
  album,
  review
  FROM MUSIC_REVIEWS;

-- =============================================================================
-- BONUS: Genre Evolution Analysis
-- =============================================================================

-- Analyze how different genres are evolving
with genre_analysis as (
SELECT 
    primary_genre,
    bio_text
FROM artists
WHERE primary_genre IS NOT NULL
)
SELECT 
    primary_genre,
    AI_AGG(bio_text, 'What are the key characteristics and evolution trends for this music genre? How are traditional elements being modernized?') as genre_trends
FROM genre_analysis
GROUP BY primary_genre;

-- =============================================================================
-- FINAL: Comprehensive Music Industry Dashboard
-- =============================================================================

CREATE OR REPLACE VIEW MUSIC_INDUSTRY_INSIGHTS AS
SELECT 
  a.artist_name,
  a.primary_genre,
  a.birthplace,
  a.instruments,
  
  -- Career stage classification
  AI_CLASSIFY(
    a.bio_text, 
    ['emerging artist', 'established artist', 'legendary artist'],
    {'task_description': 'Categorize artist by career stage'}
  ):labels[0]::string as career_stage,
  
  -- Style classification  
  AI_CLASSIFY(
    a.bio_text, 
    ['traditional', 'progressive', 'experimental'],
    {'task_description': 'Categorize musical style'}
  ):labels[0]::string as style_category,
  
  -- Venue recommendation
  (SELECT venue_name 
   FROM MUSIC_VENUES v 
   WHERE AI_FILTER(PROMPT('Is {0} a good fit for {1}?', a.bio_text, v.description)) 
   LIMIT 1) as recommended_venue,
   
  -- Review sentiment (if available)
  COALESCE(
    (SELECT AI_SENTIMENT(review) 
     FROM MUSIC_REVIEWS r 
     WHERE r.artist = a.artist_name), 
    'No reviews available'
  ) as latest_review_sentiment

FROM artists a;

SELECT * FROM MUSIC_INDUSTRY_INSIGHTS;

-- =============================================================================
-- Summary: What we've demonstrated
-- =============================================================================

/*
🎵 MUSIC INDUSTRY AI ANALYSIS COMPLETE! 🎵

This demo shows the full power of Snowflake's Cortex AI functions:

✅ AI_EXTRACT: Extract structured data from music documents (press releases, bios)
✅ AI_CLASSIFY: Categorize artists by career stage and musical style  
✅ AI_FILTER: Match artists with appropriate venues based on compatibility
✅ AI_SENTIMENT: Analyze review sentiment across multiple aspects
✅ AI_AGG: Discover industry trends and patterns across documents
✅ PARSE_DOCUMENT: Convert documents to searchable text

Featured Artists: Sarah Jarosz, Billy Strings, Sturgill Simpson, 
Kitchen Dwellers, Paul Cauthen, Greensky Bluegrass, Daniel Donato

Business Applications:
- A&R Intelligence: Identify emerging artists and trends
- Music Discovery: Extract artist similarities and influences  
- Fan Sentiment Analysis: Process reviews and social media
- Venue Analytics: Match artists to appropriate venues
- Rights Management: Extract metadata from contracts
- Festival Programming: Analyze artist compatibility

The pattern follows the exact structure of the colleague's example:
1. Stage setup and document upload
2. Single document AI_EXTRACT 
3. Batch processing multiple documents
4. Parse documents with PARSE_DOCUMENT
5. Combine parsing and extraction to create structured tables
6. Use AI_FILTER for intelligent matching
7. AI_CLASSIFY for categorization
8. AI_AGG for trend analysis across documents
9. AI_SENTIMENT for review analysis
10. Comprehensive dashboard combining all functions
*/
