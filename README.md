# MAG Data Processing

MAG Data Processing is a comprehensive pipeline designed to process data from the raw MAG database. It includes several key steps:

1. **create_field**: Creating a specialized database based on fields, conferences, and journals.
2. **compute_prob**: Computing the probabilities of papers and citations, also referred to as node and edge probabilities.
3. **topic**: Determining topics by analyzing the abstracts and titles of papers, especially those from top authors in a given field.
4. **post_processing**: Implementing post-processing steps to integrate contextual information such as abstracts, authors, and venues, making the data ready for use in GFVis.

Note that steps 2 and 3 can be processed in parallel. Additionally, the folders are named identically to their corresponding titles for ease of reference.

## Keyword Explanation

[**MAG(Microsoft Academic Graph)**](https://www.microsoft.com/en-us/research/project/microsoft-academic-graph/): The Microsoft Academic Graph is a heterogeneous graph containing scientific publication records, citation relationships between those publications, as well as authors, institutions, journals, conferences, and fields of study. This graph is used to power experiences in Bing, Cortana, Word, and in Microsoft Academic. The graph is currently being updated on a bi-weekly basis until the end of the calendar year 2021.

[**hIndex**](https://en.wikipedia.org/wiki/H-index): The h-index is an author-level metric that measures both the productivity and citation impact of the publications, initially used for an individual scientist or scholar.


[**ACL(Association for Computational Linguistics)**](https://en.wikipedia.org/wiki/Association_for_Computational_Linguistics): a scientific and professional organization for people working on natural language processing. Its namesake conference is one of the primary high impact conferences for natural language processing research, along with EMNLP.


[**ARC(ACL Anthology Reference Corpus)**](https://paperswithcode.com/dataset/acl-arc-1): an English corpus made up of conference and journal papers in natural language processing and computational linguistics. The corpus has been prepared from 18,288 papers of the ACL Anthology published in 1979–2015.



