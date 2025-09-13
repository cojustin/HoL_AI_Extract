# 🎵 Music Industry AI Analysis - Mimicking Colleague's Example
# Demonstrates AI_EXTRACT, AI_CLASSIFY, AI_SENTIMENT, AI_AGG on music data

# Import python packages
import streamlit as st
import pandas as pd
import time

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
session = get_active_session()

st.title("🎵 Music Industry AI Analysis with Snowflake Cortex")

# =============================================================================
# SETUP: Create stage and load documents
# =============================================================================

st.header("📁 Setup: Create Stage for Music Documents")

st.code("""
create or replace stage MUSIC_DOCS 
	DIRECTORY = ( ENABLE = true 
                  AUTO_REFRESH = TRUE) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );
""", language='sql')

st.markdown("**Load music documents into Snowflake stage**")

# Upload press releases
MY_STAGE = 'MUSIC_DOCS/press_releases'
MY_FILE_NAME = "data/music/press_releases/*.pdf"

# Upload the file to a stage.
put_result = session.file.put(MY_FILE_NAME, MY_STAGE, auto_compress=False, overwrite=True)

MY_STAGE = 'MUSIC_DOCS/artist_bios'
MY_FILE_NAME = "data/music/artist_bios/*.pdf"

# Upload the file to a stage.
put_result = session.file.put(MY_FILE_NAME, MY_STAGE, auto_compress=False, overwrite=True)
put_result[0].status

st.code("alter stage MUSIC_DOCS refresh;", language='sql')

# =============================================================================
# AI_EXTRACT: Single document analysis
# =============================================================================

st.header("🎸 AI_EXTRACT on Music Documents")

st.markdown("**AI_EXTRACT leverages Snowflake's Vision model Arctic-extract. We can ask questions directly on documents without OCR.**")

# Show example document
st.image('data/music/press_releases/example_press_release.png')

st.code("""
SELECT AI_EXTRACT(
  file => TO_FILE('@MUSIC_DOCS/press_releases','billy_strings_tour_2024.pdf'),
  responseFormat => [
    ['artist', 'Who is the main artist mentioned?'], 
    ['tour_dates', 'List: What tour dates are mentioned?'],
    ['venues', 'List: What venues are mentioned?']
  ]
) as json_data
""", language='sql')

st.markdown("**We can even use it for classification**")

st.code("""
SELECT AI_EXTRACT(
  file => TO_FILE('@MUSIC_DOCS/press_releases','billy_strings_tour_2024.pdf'),
  responseFormat => [['genre', 'Is this bluegrass, country, folk, or rock? Answer with one genre']]
) as json_data
""", language='sql')

# =============================================================================
# Batch processing multiple documents
# =============================================================================

st.header("📚 Batch Processing Music Documents")

st.code("""
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
""", language='sql')

# =============================================================================
# Artist biography processing
# =============================================================================

st.header("👤 Extract Information from Artist Biographies")

st.image('data/music/artist_bios/example_bio.png')

st.code("""
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
""", language='sql')

# =============================================================================
# Parse text from documents using PARSE_DOCUMENT
# =============================================================================

st.header("📄 Parse Text from Music Documents")

st.code("""
SELECT 
relative_path,
SNOWFLAKE.CORTEX.PARSE_DOCUMENT('@MUSIC_DOCS',relative_path):content::STRING AS document_text
from DIRECTORY(@MUSIC_DOCS)
where relative_path like 'artist_bios/%'
limit 5;
""", language='sql')

# =============================================================================
# Combine PARSE_DOCUMENT and AI_EXTRACT to create artists table
# =============================================================================

st.header("🗄️ Create Artists Database")

st.code("""
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
""", language='sql')

# =============================================================================
# Create Music Venues table
# =============================================================================

st.header("🏟️ Create Music Venues Database")

st.code("""
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

-- Seed data with iconic music venues
INSERT INTO MUSIC_VENUES (venue_name, venue_type, location, capacity, description) VALUES
  ('Red Rocks Amphitheatre', 'amphitheater', 'Morrison, Colorado', 9525,
   'Iconic outdoor venue carved between red sandstone formations. Known for incredible acoustics and stunning natural beauty.'),
  ('Ryman Auditorium', 'historic theater', 'Nashville, Tennessee', 2362,
   'The Mother Church of Country Music. Former home of Grand Ole Opry with perfect acoustics.'),
  ('Telluride Bluegrass Festival', 'festival', 'Telluride, Colorado', 10000,
   'Premier bluegrass festival in stunning mountain setting. Features traditional and progressive bluegrass acts.'),
  ('Austin City Limits Live', 'theater', 'Austin, Texas', 2750,
   'Modern venue based on iconic TV show. Features diverse lineup from country to indie rock.'),
  ('Newport Folk Festival', 'festival', 'Newport, Rhode Island', 10000,
   'Historic folk festival where Bob Dylan went electric. Showcases traditional and contemporary folk artists.'),
  ('The Station Inn', 'honky-tonk', 'Nashville, Tennessee', 200,
   'Intimate bluegrass venue where legends jam. Tuesday night bluegrass sessions are legendary.'),
  ('MerleFest', 'festival', 'Wilkesboro, North Carolina', 80000,
   'Traditional music festival honoring Merle Watson. Features bluegrass, folk, and Americana acts.'),
  ('Bonnaroo Music Festival', 'festival', 'Manchester, Tennessee', 80000,
   'Multi-genre festival on 700-acre farm. Features headliners across rock, country, electronic, and hip-hop.');
""", language='sql')

# =============================================================================
# AI_FILTER for artist-venue matching
# =============================================================================

st.header("🎯 AI_FILTER: Match Artists to Venues")

st.code("""
SELECT venue_name, venue_type, location, capacity, description, 
       bio_text, artist_name, primary_genre, instruments
FROM artists a
JOIN MUSIC_VENUES v
ON AI_FILTER(
  PROMPT('Would the artist described in this bio: {0} be a good fit for this venue: {1}? Consider genre, venue size, and atmosphere.', 
         a.bio_text, 
         v.description)
);
""", language='sql')

# =============================================================================
# AI_CLASSIFY artists by career stage
# =============================================================================

st.header("📊 AI_CLASSIFY: Categorize Artists")

st.code("""
Select *,
    AI_CLASSIFY(
        bio_text, 
        ['emerging artist', 'established artist', 'legendary artist'],
        {
            'task_description':'Categorize the artist based on their career achievements and recognition'
        }
    ):labels[0]::string as career_stage
from artists;
""", language='sql')

# =============================================================================
# AI_AGG to analyze trends across documents
# =============================================================================

st.header("🔍 AI_AGG: Analyze Industry Trends")

st.code("""
with parsed_press_releases as (
SELECT 
SNOWFLAKE.CORTEX.PARSE_DOCUMENT('@MUSIC_DOCS',relative_path):content::STRING AS press_text
from DIRECTORY(@MUSIC_DOCS)
where relative_path like 'press_releases/%'
)
Select 
    AI_AGG(press_text, 'What are the main trends in the music industry based on these press releases? Focus on venues, touring patterns, and collaborations.')
from parsed_press_releases;
""", language='sql')

# =============================================================================
# AI_AGG by career stage
# =============================================================================

st.header("📈 AI_AGG: Extract Patterns by Career Stage")

st.code("""
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
       AI_AGG(bio_text, 'What are common characteristics and career patterns for artists at this stage?') AS stage_analysis
  FROM career_stages
 GROUP BY career_stage;
""", language='sql')

# =============================================================================
# Sentiment Analysis on Music Reviews
# =============================================================================

st.header("😊 AI_SENTIMENT: Music Review Analysis")

# Load music reviews data
music_reviews = pd.DataFrame({
    'ARTIST': ['Billy Strings', 'Sarah Jarosz', 'Sturgill Simpson', 'Kitchen Dwellers', 'Paul Cauthen'],
    'ALBUM': ['Me/And/Dad', 'Polaroid Lovers', 'The Ballad of Dood and Juanita', 'Seven Devils', 'Country Coming Down'],
    'REVIEW': [
        'Billy Strings delivers a deeply personal and technically masterful album that honors his roots while pushing bluegrass into new territories. The guitar work is exceptional, though some tracks feel overly sentimental.',
        'Sarah Jarosz has crafted her most mature and emotionally resonant work yet. The fingerpicking is exquisite and the songwriting is vulnerable and honest. A stunning achievement in contemporary folk.',
        'Sturgill Simpson strips away the psychedelic elements for raw, acoustic storytelling. The concept is ambitious and mostly successful, though the pacing drags in the middle section.',
        'Kitchen Dwellers blend traditional bluegrass with modern sensibilities perfectly. The energy is infectious and the musicianship is tight, but the production could be cleaner.',
        'Paul Cauthen brings theatrical flair and genuine emotion to Texas country. His voice is powerful and the songs are well-crafted, though the album feels slightly repetitive by the end.'
    ]
})

music_reviews_sf = session.create_dataframe(music_reviews)
music_reviews_sf.write.mode("overwrite").save_as_table("MUSIC_REVIEWS")

st.code("select * from MUSIC_REVIEWS", language='sql')

st.markdown("**Basic sentiment analysis:**")

st.code("""
SELECT
  AI_SENTIMENT(
    review
  ),
  artist,
  album,
  review
  FROM MUSIC_REVIEWS;
""", language='sql')

st.markdown("**Multi-aspect sentiment analysis for detailed insights:**")

st.code("""
SELECT
  AI_SENTIMENT(
    review,
    ['musicianship', 'songwriting', 'production', 'vocals', 'innovation']
  ),
  artist,
  album,
  review
  FROM MUSIC_REVIEWS;
""", language='sql')

# =============================================================================
# Final Summary
# =============================================================================

st.markdown("---")
st.header("🎵 Music Industry AI Analysis Complete!")

st.markdown("""
This demo shows the full power of Snowflake's Cortex AI functions for music industry analysis:

- **AI_EXTRACT**: Extract structured data from music documents (press releases, bios, reviews)
- **AI_CLASSIFY**: Categorize artists by career stage and musical style
- **AI_FILTER**: Match artists with appropriate venues based on compatibility
- **AI_SENTIMENT**: Analyze review sentiment across multiple aspects (musicianship, songwriting, etc.)
- **AI_AGG**: Discover industry trends and patterns across multiple documents
- **PARSE_DOCUMENT**: Convert documents to searchable text for further analysis

**Featured Artists:** Sarah Jarosz, Billy Strings, Sturgill Simpson, Kitchen Dwellers, Paul Cauthen, Greensky Bluegrass, Daniel Donato

**Use Cases:**
- A&R Intelligence: Identify emerging artists and trends
- Music Discovery: Extract artist similarities and influences  
- Fan Sentiment Analysis: Process reviews and social media
- Venue Analytics: Match artists to appropriate venues
- Rights Management: Extract metadata from contracts
""")

st.markdown("*Built with ❤️ using Snowflake Cortex AI*")
