export fieldID=77088390
export database=scigene_database_field

python extract_scigene_field.py
python extract_citation_timeseries.py
python update_authors_field.py
# python set_fellow.py $2