# compute probability

Computing the probabilities of papers and citations, also referred to as node and edge probabilities.

## pipeline

The pipeline is defined in `run.sh`. Before execution, modify `export database=scigene_VCG_field` to target the specified field database. The default topN is set to 5000, indicating that the process will focus on the top 5000 authors sorted by hIndex.

1. **compute_key_papers.py**
   - Calculates the probability of papers for the specified author database.
2. **compute_key_citation**
   - **compute_similarity_features.py** - Generates similarity features.
   - **run_extract_features.py** - Extracts necessary features.
   - **compute_link_prob.py** - Calculates the probability of citations/links.
3. **analyse_distribution.py**
   - Analyzes the distribution of generated probabilities for papers and citations, and creates corresponding visualizations.