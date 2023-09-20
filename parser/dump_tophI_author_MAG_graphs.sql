select * from scigene_$$fieldNAME$$_field.authors_field_hI 
where authorRank <= $$NUMTOPAUTHORS$$ 
INTO OUTFILE '/home/leishi/scigene/dataset/MAG/parser/data/csv/$$fieldNAME$$_hI/top_field_authors.csv' 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'

select authorID, name, authorRank, PaperCount_field 
from scigene_$$fieldNAME$$_field.authors_field_hI 
where authorRank <= $$NUMTOPAUTHORS$$;

select * from papers_$$fieldAUTHOR$$ 
INTO OUTFILE '/home/leishi/scigene/dataset/MAG/parser/data/csv/$$fieldNAME$$_hI/papers_$$fieldAUTHOR$$.csv' 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'

select * from links_$$fieldAUTHOR$$ 
INTO OUTFILE '/home/leishi/scigene/dataset/MAG/parser/data/csv/$$fieldNAME$$_hI/links_$$fieldAUTHOR$$.csv' 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\\n'