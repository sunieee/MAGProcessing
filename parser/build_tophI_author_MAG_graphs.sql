-- 按hIndex_field降序建立表authors_field_hI，其中authorRank每增加一行+1
create table scigene_$$fieldNAME$$_field.authors_field_hI 
select authorID, `rank`, name, PaperCount, CitationCount, PaperCount_field, 
( @row_number := @row_number + 1 ) AS authorRank, CitationCount_field, hIndex_field, FellowType 
from scigene_$$fieldNAME$$_field.authors_field order by hIndex_field desc;

-- 选取前NUMTOPAUTHORS个学者
select authorID, name, authorRank, PaperCount_field 
from scigene_$$fieldNAME$$_field.authors_field_hI 
where authorRank <= $$NUMTOPAUTHORS$$;

-- 创建表papers_xiaofengli1：paperID, title, year, referenceCount, citationCount, authorOrder, isKeyPaper, firstAuthorID, firstAuthorName
drop table papers_xiaofengli1
create table papers_$$fieldAUTHOR$$ 
(firstAuthorID varchar(15), firstAuthorName varchar(999), isKeyPaper float) 
select papers_field.paperID, title, year, referenceCount, citationCount, 
min(authorOrder) as authorOrder, 0 as isKeyPaper, '' as firstAuthorID, '' as firstAuthorName 
from scigene_$$fieldNAME$$_field.paper_author_field, scigene_$$fieldNAME$$_field.papers_field 
where authorID = ? and papers_field.paperID = paper_author_field.paperID 
group by papers_field.paperID, title, year

-- 表paper_author_field中authorOrder = 1的authorID为firstAuthorID
update papers_$$fieldAUTHOR$$ as P, scigene_$$fieldNAME$$_field.paper_author_field as PA 
set P.firstAuthorID = PA.authorID where P.paperID = PA.paperID and PA.authorOrder = 1
-- 更新firstAuthorName
update papers_$$fieldAUTHOR$$ as P, scigene_$$fieldNAME$$_field.authors_field_hI as A 
set P.firstAuthorName = A.name where P.firstAuthorID = A.authorID

-- 创建links_xiaofengli1：citingpaperID, citedpaperID, sharedAuthor, extends_prob。其中citingpaperID均来自papers_xiaofengli1
drop table links_$$fieldAUTHOR$$
create table links_$$fieldAUTHOR$$ (extends_prob float) 
select P.citingpaperID, P.citedpaperID, 0 as sharedAuthor, null as extends_prob 
from scigene_$$fieldNAME$$_field.paper_reference_field as P 
where P.citingpaperID in (select paperID from papers_$$fieldAUTHOR$$) 
group by P.citingpaperID, P.citedpaperID

-- citingpaperID和citedpaperID均属于xiaofengli1，则sharedAuthor=1
update links_$$fieldAUTHOR$$ as P, scigene_$$fieldNAME$$_field.paper_author_field as A, scigene_$$fieldNAME$$_field.paper_author_field as B 
set P.sharedAuthor = 1 
where A.paperID = P.citingpaperID and B.paperID = P.citedpaperID and A.authorID = B.authorID
-- 从paper_reference_field_labeled更新extends_prob
update links_$$fieldAUTHOR$$, scigene_$$fieldNAME$$_field.paper_reference_field_labeled 
set links_$$fieldAUTHOR$$.extends_prob = paper_reference_field_labeled.extends_prob 
where links_$$fieldAUTHOR$$.citingpaperID = paper_reference_field_labeled.citingpaperID 
and links_$$fieldAUTHOR$$.citedpaperID = paper_reference_field_labeled.citedpaperID