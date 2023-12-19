# compute probability

Computing the probabilities of papers and citations, also referred to as node and edge probabilities.

## pipeline

The pipeline is defined in `run.sh`. Before execution, it is necessary to:
- change `user, password` to `visitor, visdata2023`
- change `export database=scigene_VCG_field` to the specified field database, use `visitor` as prefix, like `export database=visitor_HCI_field`.

The default topN is set to 5000, indicating that the process will focus on the top 5000 authors sorted by hIndex.

1. **compute_key_papers.py**
   - **compute_key_papers.py** - Calculates the probability of papers for the specified author database.
   - **update_papers.py** - Implementing post-processing steps to integrate contextual information such as abstracts, authors, and venues, making the data ready for use in GFVis.
2. **compute_key_citation**
   - **compute_similarity_features.py** - Generates similarity features.
   - **run_extract_features.py** - Extracts necessary features.
   - **compute_link_prob.py** - Calculates the probability of citations/links.
   - **update_links.py** - fetch_citation_context
3. **analyse_distribution.py**
   - Analyzes the distribution of generated probabilities for papers and citations, and creates corresponding visualizations.


注意：`paperID2abstract.json` 要删掉，visualization领域因为没有删除而使用了旧的abstract，造成学习topic文章数更少