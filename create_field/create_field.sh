
python extract_scigene_field.py $1 $2
python extract_citation_timeseries.py $1 $2
python update_top_author_field_hIndex.py $1 $2
# python set_fellow.py $2