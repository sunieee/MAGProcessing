# compute probability

Implementing post-processing steps to integrate contextual information such as abstracts, authors, and venues, making the data ready for use in GFVis.

## pipeline

The pipeline is defined in `run.sh`. Before execution, modify `export database=scigene_VCG_field` to target the specified field database. The pipeline first move all the information from `compute_prob` and `topic` and integrate them together.

1. **update_links.py**
   - fetch_citation_context
   
2. **update_papers.py**
   - extract_paper_authors
   - extract_paper_venu
   - add abstract
   - add topic