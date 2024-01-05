create database MACG;
show tables in MAG;
rename table MAG.field_of_study to MACG.field_of_study;
show tables in MACG;
select count(*) from MACG.field_of_study;
show tables in MAG:
rename table MAG.field_children to MACG.field_children;
rename table MAG.authors to MACG.authors;
rename table MAG.papers to MACG.papers;
rename table MAG.paper_author to MACG.paper_author;
rename table MAG.paper_field to MACG.paper_field;
rename table MAG.paper_reference to MACG.paper_reference;
drop database MAG;


create database scigene_visualization_field_old;
rename table 