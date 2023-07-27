require(tidyverse)

# Download a sampe of source files -------------

# master list of all GKG files ever..
ml = read_delim('http://data.gdeltproject.org/gdeltv2/masterfilelist.txt', 
                delim = ' ', col_names = c('size','md5','url')) |> 
  mutate(ts = str_extract(url, '(?<=/)[0-9]{12}') |> 
           as.POSIXct(format='%Y%m%d%H%M', tz = 'UTC')) |>
  mutate(series = str_extract(url, ('(?<=[0-9][.]).*?(?=[.])')))

N = 100 # sample of files (15 mins coverage each)

s = ml |> filter(ts > '2022-01-01') |> 
  filter(series == 'gkg') |> sample_n(N) |> arrange(ts)

dir.create('raw', showWarnings = FALSE)
unlink('raw/*') # delete any existing files

for(i in 1:length(s$url)){
  f = s$url[i]
  try(download.file(f, destfile = paste0('raw/', str_extract(f, '[^/]+$'))))
}

# Process each file and write to csv -------------

# file list
fl = list.files('raw/', pattern = '.zip', full.names = TRUE)

col_names = c('GKGRECORDID', 'V2.1DATE', 'V2SOURCECOLLECTIONIDENTIFIER', 'V2SOURCECOMMONNAME',
              'V2DOCUMENTIDENTIFIER', 'V1COUNTS', 'V2.1COUNTS', 'V1THEMES', 'V2ENHANCEDTHEMES',
              'V1LOCATIONS', 'V2ENHANCEDLOCATIONS', 'V1PERSONS', 'V2ENHANCEDPERSONS',
              'V1ORGANIZATIONS', 'V2ENHANCEDORGANIZATIONS', 'V1.5TONE', 'V2.1ENHANCEDDATES',
              'V2GCAM', 'V2.1SHARINGIMAGE', 'V2.1RELATEDIMAGES', 'V2.1SOCIALIMAGEEMBEDS',
              'V2.1SOCIALVIDEOEMBEDS', 'V2.1QUOTATIONS', 'V2.1ALLNAMES', 'V2.1AMOUNTS',
              'V2.1TRANSLATIONINFO', 'V2EXTRASXML')

# a generic function for parsing most of GKG's 'V2' fields
parse_v2 = function(x, urls, cat){
  str_split(x, ';') |> map(~ str_subset(.x, '.')) |> 
    map(~ enframe(.x, name = NULL)) |> `names<-`(urls) |> 
    bind_rows(.id = 'url') |> 
    separate(value, c('value','char_offset'), ',') |> 
    group_by(url, value) |> 
    summarise(n = length(url),
              char_offset = paste(char_offset, collapse=','), 
              .groups = 'drop') |> 
    group_by(url) |> mutate(rank = dplyr::min_rank(-n)) |> ungroup() |> 
    arrange(url, rank) |> 
    mutate(variable = NA_character_, .before = value) |> 
    mutate(category = cat, .before = variable)
}

# output file
fn = 'sample.csv'

# write header row
tibble(timestamp = character(0), url = character(0),  category = character(0), 
       variable = character(0), value = character(0), 
       char_offset = character(0), rank = character(0)) |>
  write_csv(fn)


for(i in 1:length(fl)){
  
  message(i)
  f = fl[i]
  d = read_tsv(f, col_names = col_names, col_types = cols(.default = col_character())) |> 
    mutate(timestamp = as.POSIXct(V2.1DATE, format = '%Y%m%d%H%M%S', tz = 'UTC'))
  
  locs = str_split(d$V2ENHANCEDLOCATIONS, ';') |> map(enframe, name = NULL) |> 
    `names<-`(d$V2DOCUMENTIDENTIFIER) |>
    bind_rows(.id = 'url') |> 
    separate(value, c('type','name','cc','adm1','adm2','lat','lon','feat_id','char_offset'), '#') |>
    select(-c(cc:feat_id)) |> 
    rename(variable = type, value = name) |> 
    group_by(url, variable, value) |> 
    summarise(n = length(url),
              char_offset = paste(char_offset, collapse=','), 
              .groups = 'drop') |> 
    mutate(category = 'geo', .before = variable) |> 
    group_by(url) |> mutate(rank = dplyr::min_rank(-n)) |> ungroup() |>
    arrange(url, rank) |> 
    filter(!is.na(variable))

  extras = d$V2EXTRASXML |> str_split('><') |> map(enframe, name = NULL) |> 
    `names<-`(d$V2DOCUMENTIDENTIFIER) |> 
    bind_rows(.id = 'url') |> 
    mutate(value = str_remove(value, '^<') |> str_remove('</.*')) |> 
    separate(value, c('variable', 'value'), sep = '>', fill = 'right') |> 
    mutate(category = 'extras', .before = variable) |> 
    filter(!str_starts(variable, '/'), !is.na(value))
    
  all_tags = list(
    parse_v2(d$V2ENHANCEDPERSONS, d$V2DOCUMENTIDENTIFIER, 'person'),
    parse_v2(d$V2ENHANCEDORGANIZATIONS, d$V2DOCUMENTIDENTIFIER, 'org'),
    parse_v2(d$V2ENHANCEDTHEMES, d$V2DOCUMENTIDENTIFIER, 'theme') |> 
      mutate(variable = str_remove(value, '_.*')),
    locs,
    extras
  ) |> bind_rows() |> 
    arrange(url, -n, rank) |> select(-n) |> 
    left_join(select(d, V2DOCUMENTIDENTIFIER, timestamp), by = c(url = 'V2DOCUMENTIDENTIFIER')) |> 
    relocate(timestamp, 1)
  
  write_csv(all_tags, fn, append = TRUE)
}

