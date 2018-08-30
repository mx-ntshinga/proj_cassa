/* Cassandra CQL language */
/* UDT - USER DEFINED TYPES - For nested data structures */

CREATE TYPE IF NOT EXISTS user_mentions_tp (id bigint,id_str text,indices list<int>,screen_name text,name text); 

CREATE TYPE IF NOT EXISTS hashtagz   (indices list<int>,text text);

CREATE TYPE IF NOT EXISTS sizes_tp   (w  int,h  int,resize  text);

CREATE TYPE IF NOT EXISTS options_tp (position int,text text);

CREATE TYPE IF NOT EXISTS polls_tp   (end_datetime text,duration_minutes int,options list<frozen<options_tp>>);

CREATE TYPE IF NOT EXISTS add_media_info_tp (title text,description text,embeddable boolean,monetizable boolean);

CREATE TYPE IF NOT EXISTS variant_tp     (url text, bitrate bigint, content_type text);

CREATE TYPE IF NOT EXISTS video_info_tp (aspect_ratio list<int>, duration_millis int, variants frozen<list<variant_tp>>);

CREATE TYPE IF NOT EXISTS media_tp 
(
    id bigint, id_str text, indices list<int>, media_url text, media_url_https text, 
    display_url text, expanded_url  text, type text, sizes map <text, frozen<sizes_tp>>,
    source_status_id bigint, source_user_id bigint, source_status_id_str text,  url text,
    source_user_id_str text, additional_media_info frozen<add_media_info_tp>,
    video_info frozen<video_info_tp>, description text
);
CREATE TYPE IF NOT EXISTS symbols_tp (indices list<int>, text text);

CREATE TYPE IF NOT EXISTS entities_tp (
    symbols frozen<list<symbols_tp>>,
    media frozen<list<media_tp>>,
    polls frozen<polls_tp>,
    urls frozen<list<media_tp>>,
    hashtags frozen<list<hashtagz>>,
    user_mentions frozen<list<user_mentions_tp>>
);
CREATE TYPE IF NOT EXISTS coordinates_tp (coordinates frozen<list<float>>, type text,);

CREATE TYPE IF NOT EXISTS coordinates_tp2 (coordinates frozen<list<list<list<float>>>>, type text);

CREATE TYPE IF NOT EXISTS place_tp (
   	id text, url text, name text, country text, full_name text, place_type text,
   	country_code text, attributes map<text,text>, bounding_box frozen<coordinates_tp2>
);
CREATE TYPE IF NOT EXISTS extended_entities_tp (media frozen<list<media_tp>>);

CREATE TYPE IF NOT EXISTS matching_rule (tag text, id bigint, id_str text);

CREATE TYPE IF NOT EXISTS current_user_retweet_tp (id bigint, id_str text);

CREATE TYPE IF NOT EXISTS extended_tweet_tp (full_text text, display_text_range list<int>, entities frozen<entities_tp>, extended_entities frozen<entities_tp>);

CREATE TYPE IF NOT EXISTS geo_tp (type text, coordinates frozen<list<float>>);

CREATE TYPE IF NOT EXISTS users (
    id bigint,              verified boolean,   follow_request_sent text,   
    followers_count int,    id_str bigint, 	 listed_count int, 	          utc_offset bigint,
    statuses_count int,     description text,   friends_count int,        location text,
    favourites_count int,   screen_name text,    notifications text,      profile_background_color text,
    name text,              lang text,          url text,                 created_at text,      		
    time_zone text,         protected boolean,   translator_type text,    is_translator boolean, 
    following text,         geo_enabled boolean, default_profile boolean, profile_link_color text,
    profile_image_url text, profile_sidebar_fill_color text,   profile_text_color text,
    profile_banner_url text,profile_background_image_url text,  profile_background_tile boolean,
    contributors_enabled boolean,  profile_sidebar_border_color text,   profile_background_image_url_https text,
    default_profile_image boolean, profile_image_url_https text, profile_use_background_image boolean
);

CREATE TYPE IF NOT EXISTS Tweet_structure (
    created_at text,
    id bigint,
    id_str text,
    text text,
    filter_level text,
    lang text, 
    source text,
    geo frozen<geo_tp>,
    timestamp_ms bigint,
    truncated boolean,
    contributors text,
    quote_count bigint,
    reply_count bigint,
    retweet_count bigint,
    favorite_count bigint,
    favorited boolean,
    retweeted boolean,
    is_quote_status boolean,
    quoted_status_id bigint,
    possibly_sensitive boolean,
    in_reply_to_status_id bigint,
    in_reply_to_status_id_str text,
    in_reply_to_user_id bigint,
    in_reply_to_user_id_str text,
    in_reply_to_screen_name text,
    quoted_status_id_str text,
    display_text_range list<int>,

    entities frozen<entities_tp>,
    matching_rules frozen<list<matching_rule>>,
    extended_entities frozen<extended_entities_tp>,
    place frozen<place_tp>,
    coordinates frozen<coordinates_tp>,
     
    current_user_retweet frozen<current_user_retweet_tp>,
    scopes map<text, boolean>,
    withheld_copyright boolean,
    withheld_in_countries list<text>,
    withheld_scope text,
    extended_tweet frozen<extended_tweet_tp>,
    quoted_status text,
    user frozen<users>,
);